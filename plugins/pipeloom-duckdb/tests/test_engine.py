"""DuckDB specifics: the spec imports no DuckDB, model catalogs are sibling files, the extension
policy (PLX121) and read-only Delta."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest
from tests.conftest import make_project

from pipeloom.api import Project, ProjectOptions
from pipeloom.engine.types import Column, ReadSpec, TargetRef, WriteConfig
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from pipeloom_duckdb.catalog import DuckDBCatalog
from pipeloom_duckdb.engine import DuckDBEngine, DuckDBSession
from pipeloom_duckdb.extensions import ExtensionMissingError
from pipeloom_duckdb.spec import DuckDBEngineOptions

from .conftest import extension_dir, write_source  # type: ignore[import-not-found]

SPEC_CODE = """
import sys
import pipeloom_duckdb.spec as spec
assert spec.ENGINE_SPEC.capabilities.sql_dialect == "duckdb"
assert "duckdb" not in sys.modules, "duckdb was imported"
assert "pipeloom_duckdb.engine" not in sys.modules
print("ok")
"""

SCHEMA = (Column("id", "int64"), Column("label", "string"))
ROWS = ({"id": 1, "label": "a"}, {"id": 2, "label": None})


def _ctx(tmp_path: Path) -> RuntimeContext:
    return RuntimeContext(
        run_id="r",
        invocation_id="r",
        project="p",
        target="t",
        project_dir=tmp_path,
        clock=FakeClock(),
        identity=Identity("tester", "host"),
        vars={},
    )


def test_spec_import_does_not_import_duckdb() -> None:
    proc = subprocess.run(
        [sys.executable, "-I", "-c", SPEC_CODE], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"


def test_a_model_catalog_is_a_sibling_file(tmp_path: Path) -> None:
    main = tmp_path / "wh" / "main_db.duckdb"
    catalog = DuckDBCatalog(main.as_posix())
    target = TargetRef("silver", "tpch", "orders")
    with DuckDBEngine().session(_ctx(tmp_path), catalog=catalog) as s:
        assert not catalog.exists(target)
        catalog.ensure_namespace(target.namespace)
        rel = s.from_rows(ROWS, SCHEMA)
        s.materializer("overwrite").materialize(rel, target, WriteConfig("overwrite"), "t1")
    assert (tmp_path / "wh" / "silver.duckdb").is_file()
    # a new session attaches the existing sibling, so the table reads back
    with DuckDBEngine().session(_ctx(tmp_path), catalog=catalog) as s:
        got = s.to_rows(s.reader("table").read(ReadSpec("table", table=target)))
    assert sorted(r["id"] for r in got) == [1, 2]


def test_a_memory_catalog_holds_model_catalogs_in_memory(tmp_path: Path) -> None:
    catalog = DuckDBCatalog(":memory:")
    target = TargetRef("gold", "main", "t")
    with DuckDBEngine().session(_ctx(tmp_path), catalog=catalog) as s:
        catalog.ensure_namespace(target.namespace)
        s.materializer("append").materialize(
            s.from_rows(ROWS, SCHEMA), target, WriteConfig("append"), "t1"
        )
        assert catalog.table_schema(target) == SCHEMA
    assert list(tmp_path.iterdir()) == []


def test_a_missing_extension_is_plx121_and_permanent(tmp_path: Path) -> None:
    empty = tmp_path / "ext"
    empty.mkdir()
    table = tmp_path / "delta_src"
    write_source("delta", table, ROWS, SCHEMA)
    engine = DuckDBEngine(DuckDBEngineOptions(extension_directory=empty.as_posix()))
    with engine.session(_ctx(tmp_path), catalog=DuckDBCatalog(":memory:")) as s:
        with pytest.raises(ExtensionMissingError) as err:
            s.reader("delta").read(ReadSpec("delta", location=table.as_posix()))
        assert s.is_transient(err.value) is False
    assert err.value.diagnostic.code == "PLX121"
    assert str(err.value).startswith("PLX121 DuckDB extension 'delta' is not installed")
    assert list(empty.iterdir()) == []  # nothing was downloaded


def test_a_listed_extension_fails_on_first_use_not_at_open(tmp_path: Path) -> None:
    engine = DuckDBEngine(
        DuckDBEngineOptions(
            extension_directory=(tmp_path / "ext").as_posix(), extensions=("delta",)
        )
    )
    with (
        engine.session(_ctx(tmp_path), catalog=DuckDBCatalog(":memory:")) as s,
        pytest.raises(ExtensionMissingError),
    ):
        s.count(s.from_rows(ROWS, SCHEMA))


def test_a_run_reports_plx121_inside_plm204(tmp_path: Path) -> None:
    root = make_project(
        tmp_path / "p",
        {
            "pipeloom.yml": (
                "config_version: 1\nname: shop\n"
                "sources:\n  raw:\n    format: delta\n    tables:\n"
                "      events: {location: data/events}\n"
                "targets:\n  dev:\n"
                "    engine: {kind: duckdb, extension_directory: ext}\n"
                "    catalog: {kind: duckdb, path: target/warehouse.duckdb}\n"
                "    state: {kind: json, path: .pipeloom/state}\n    naming: {kind: none}\n"
            ),
            "models/m.py": (
                "from pipeloom import model\n\n\n@model\n"
                "def copy(ctx, events):\n    return ctx.sql('select * from events')\n"
            ),
        },
    )
    write_source("delta", root / "data" / "events", ROWS, SCHEMA)
    project = Project(ProjectOptions(project_dir=root, target="dev", env={}), clock=FakeClock())
    assert project.run(RunOptions(run_id="r1", workers=1)).exit_code == 1
    path = root / "target" / "runs" / "r1" / "run_results.json"
    (row,) = json.loads(path.read_bytes())["results"]
    assert row["error"]["code"] == "PLM204"
    assert "PLX121 DuckDB extension 'delta'" in row["error"]["message"]
    assert row["error"]["category"] == "permanent"


@pytest.mark.duckdb_extensions
def test_delta_reads_offline_from_the_pinned_directory(tmp_path: Path) -> None:
    """Runs where the `delta` extension is preinstalled; install_extensions stays off, so the
    read never reaches the network."""
    directory = extension_dir()
    assert directory is not None
    table = tmp_path / "delta_src"
    write_source("delta", table, ROWS, SCHEMA)
    engine = DuckDBEngine(DuckDBEngineOptions(extension_directory=directory))
    with engine.session(_ctx(tmp_path), catalog=DuckDBCatalog(":memory:")) as s:
        rel = s.reader("delta").read(ReadSpec("delta", location=table.as_posix()))
        assert s.schema(rel) == SCHEMA
        assert sorted(s.to_rows(rel), key=lambda r: r["id"]) == list(ROWS)
        assert isinstance(s, DuckDBSession)
        installed = s.rows(
            "SELECT installed, loaded FROM duckdb_extensions() WHERE extension_name = 'delta'"
        )
    assert installed == [(True, True)]


def test_the_engine_reports_the_duckdb_version() -> None:
    assert DuckDBEngine().version == duckdb.__version__
