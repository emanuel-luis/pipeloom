"""scd2 through the runner on polars: versions across runs, deletes, and the PLM206 guards."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.engine.types import ReadSpec, TargetRef
from pipeloom.events.bus import EventBus
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project

PIPELOOM = """config_version: 1
name: p
python_paths:
  - src
targets:
  ci:
    engine:
      kind: polars
    catalog:
      kind: filesystem
    state:
      kind: json
      path: state
    naming:
      kind: none
"""
SCD2 = """models:
  - name: dim
    python: "m:dim"
    inputs:
      raw:
        source: raw
    write:
      strategy: STRATEGY
      unique_key:
        - id
      deletes:
        column: op
        value: D
"""
SOURCES = """sources:
  - name: raw
    format: csv
    location: raw.csv
"""
SRC = "def dim(ctx, raw):\n    return raw\n"
DAY = [datetime(2026, 3, d, tzinfo=UTC) for d in (1, 2, 3, 4)]


def project(tmp_path: Path, csv: str, strategy: str = "scd2") -> Path:
    pytest.importorskip("pipeloom_polars")
    return make_project(
        tmp_path,
        {
            "pipeloom.yml": PIPELOOM,
            # unique_key and deletes would be PLP405 on overwrite
            "models/m.yml": SCD2.replace("STRATEGY", strategy)
            if strategy == "scd2"
            else SCD2.replace("STRATEGY", strategy).split("      unique_key:")[0],
            "models/s.yml": SOURCES,
            "src/m.py": SRC,
            "raw.csv": csv,
        },
    )


def run(root: Path, run_id: str, day: int, *, full_refresh: bool = False) -> RunOutcome:
    clock = FakeClock(DAY[day], step=timedelta(0))
    proj = Project(ProjectOptions(project_dir=root, target="ci"), clock=clock)
    with EventBus([], clock=clock, invocation_id="inv") as bus:
        return proj.run(RunOptions(run_id=run_id, full_refresh=full_refresh), bus)


def counts(root: Path, run_id: str) -> tuple[Any, ...]:
    doc = json.loads((root / "target" / "runs" / run_id / "run_results.json").read_bytes())
    (r,) = doc["results"]
    return (r["rows_inserted"], r["rows_updated"], r["rows_deleted"], r["rows_written"])


def table(root: Path) -> list[tuple[Any, ...]]:
    from pipeloom_polars.engine import PolarsEngine

    ctx = RuntimeContext(
        run_id="t",
        invocation_id="t",
        project="p",
        target="ci",
        project_dir=root,
        clock=FakeClock(),
        identity=Identity("tester", "host"),
        vars={},
    )
    with PolarsEngine().session(ctx, catalog=FilesystemCatalog(root)) as s:
        rel = s.reader("table").read(ReadSpec("table", table=TargetRef(None, "main", "dim")))
        rows = rel.native.collect().to_dicts()
    return sorted(
        (r["id"], r["name"], r["valid_from"], r["valid_to"], r["is_current"]) for r in rows
    )


def test_versions_across_runs(tmp_path: Path) -> None:
    root = project(tmp_path, "id,name,op\n1,a,\n2,b,\n")
    assert run(root, "r1", 0).exit_code == 0
    assert counts(root, "r1") == (2, 0, 0, 2)
    (root / "raw.csv").write_text("id,name,op\n1,a2,\n2,b,D\n3,c,\n", encoding="utf-8")
    assert run(root, "r2", 1).exit_code == 0
    assert counts(root, "r2") == (2, 1, 1, 4)
    want = [
        (1, "a", DAY[0], DAY[1], False),
        (1, "a2", DAY[1], None, True),
        (2, "b", DAY[0], DAY[1], False),
        (3, "c", DAY[1], None, True),
    ]
    assert table(root) == want
    outcome = run(root, "r3", 2)  # the same batch again: the delete of a closed key is ignored
    assert outcome.exit_code == 0, outcome.diagnostics
    assert counts(root, "r3") == (0, 0, 0, 0)
    assert table(root) == want


def test_a_full_refresh_drops_the_history(tmp_path: Path) -> None:
    root = project(tmp_path, "id,name,op\n1,a,\n")
    assert run(root, "r1", 0).exit_code == 0
    (root / "raw.csv").write_text("id,name,op\n1,a2,\n", encoding="utf-8")
    assert run(root, "r2", 1).exit_code == 0
    assert run(root, "r3", 2, full_refresh=True).exit_code == 0
    assert table(root) == [(1, "a2", DAY[2], None, True)]
    doc = json.loads((root / "target" / "runs" / "r3" / "run_results.json").read_bytes())
    assert doc["results"][0]["schema_changes"] == []  # no system column reads as removed


def test_a_target_without_system_columns_needs_a_full_refresh(tmp_path: Path) -> None:
    root = project(tmp_path, "id,name,op\n1,a,\n", strategy="overwrite")
    assert run(root, "r1", 0).exit_code == 0
    (root / "models" / "m.yml").write_text(SCD2.replace("STRATEGY", "scd2"), encoding="utf-8")
    outcome = run(root, "r2", 1)
    errors = [d for d in outcome.diagnostics if d.severity == "error"]
    assert [d.code for d in errors] == ["PLM206"]
    assert "--full-refresh" in str(errors[0].context) + errors[0].message + str(errors[0].hint)
    assert run(root, "r3", 2, full_refresh=True).exit_code == 0
    assert table(root) == [(1, "a", DAY[2], None, True)]


def test_a_batch_with_a_system_column_is_plm206(tmp_path: Path) -> None:
    root = project(tmp_path, "id,name,op,is_current\n1,a,,x\n")
    outcome = run(root, "r1", 0)
    assert [d.code for d in outcome.diagnostics if d.severity == "error"] == ["PLM206"]
