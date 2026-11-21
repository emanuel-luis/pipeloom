"""ctx.sql on pandas runs through DuckDB when it is installed, and is refused clearly when not."""

from __future__ import annotations

import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest
from tests.conftest import make_project

from pipeloom.api import Project, ProjectOptions
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from pipeloom_pandas import sql
from pipeloom_pandas.spec import CAPABILITIES

pytest.importorskip("duckdb")

FILES = {
    "pipeloom.yml": (
        "config_version: 1\nname: shop\ntargets:\n  dev:\n"
        "    engine:\n      kind: pandas\n    catalog:\n      kind: filesystem\n"
        "      root: wh\n"
    ),
    "models/m.py": (
        "from pipeloom import model\n\n\n"
        "@model\ndef a(ctx):\n    return ctx.sql('select 1 as id union all select 2 as id')\n\n\n"
        "@model\ndef b(ctx, a):\n    return ctx.sql('select id * 10 as id from a where id > 1')\n"
    ),
}

# The spec without DuckDB installed.
NO_DUCKDB = """
import sys

sys.modules["duckdb"] = None  # find_spec returns None for a module set to None
import pipeloom_pandas.spec as spec
assert spec.SPEC.capabilities.sql_dialect is None
assert not spec.SPEC.capabilities.sql_models
from pipeloom_pandas.engine import PandasSession
print("ok")
"""


def test_pandas_runs_sql_in_the_duckdb_dialect_when_duckdb_is_installed() -> None:
    assert CAPABILITIES.sql_dialect == "duckdb"
    assert CAPABILITIES.sql_models


def test_without_duckdb_the_engine_declares_no_sql() -> None:
    proc = subprocess.run(
        [sys.executable, "-I", "-c", NO_DUCKDB], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"


def test_a_project_runs_ctx_sql_on_pandas(tmp_path: Path) -> None:
    make_project(tmp_path, FILES)
    project = Project(ProjectOptions(project_dir=tmp_path, env={}), clock=FakeClock())
    result = project.run(RunOptions(run_id="r", workers=1))
    codes = {d.code for d in result.diagnostics}
    assert "PLK606" not in codes and "PLX101" not in codes, codes
    files = [f for f in (tmp_path / "wh").rglob("*.parquet") if "b" in f.parts]
    assert files, sorted(str(f) for f in (tmp_path / "wh").rglob("*"))
    assert pd.concat(pd.read_parquet(f) for f in files)["id"].tolist() == [20]


def test_dtypes_stay_arrow_backed_and_timestamps_stay_utc() -> None:
    frame = pd.DataFrame(
        {
            "id": pd.array([1, None], dtype=pd.ArrowDtype(pa.int64())),
            "ts": pd.array(
                [datetime(2026, 1, 1, tzinfo=UTC), None],
                dtype=pd.ArrowDtype(pa.timestamp("us", tz="UTC")),
            ),
        }
    )
    out = sql.run("select id, ts from src", {"src": frame})
    assert str(out["id"].dtype) == "int64[pyarrow]"
    assert out["ts"].dtype == pd.ArrowDtype(pa.timestamp("us", tz="UTC"))
    assert out["id"].isna().tolist() == [False, True]


def test_an_alias_never_leaks_into_the_next_query() -> None:
    frame = pd.DataFrame({"id": [1]})
    sql.run("select * from src", {"src": frame})
    with pytest.raises(Exception, match="src"):
        sql.run("select * from src", {})
