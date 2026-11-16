"""A model catalog on the filesystem catalog lands under `<root>/<catalog>/<schema>/<name>/`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from tests.conftest import make_project

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink

PROJECT = """config_version: 1
name: lay
targets:
  dev:
    engine: {kind: polars}
    catalog: {kind: filesystem, root: wh}
    state: {kind: json, path: .pipeloom/state}
    naming: {kind: none}
"""
MODELS = """models:
  - {name: a, sql: "select 1 as id", catalog: bronze, schema: raw}
  - {name: b, sql: "select 2 as id"}
"""


def test_model_catalog_is_a_leading_directory(tmp_path: Path) -> None:
    make_project(tmp_path, {"pipeloom.yml": PROJECT, "models/m.yml": MODELS})
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    sink = RecordingSink()
    proj = Project(ProjectOptions(project_dir=tmp_path, env={}), clock=clock, invocation_id="i")
    with EventBus([(sink, 5)], clock=clock, invocation_id="i") as bus:
        outcome = proj.run(RunOptions(run_id="r1", workers=1), bus)
    assert outcome.exit_code == 0, outcome.diagnostics
    assert list((tmp_path / "wh" / "bronze" / "raw" / "a").glob("*.parquet"))
    assert list((tmp_path / "wh" / "main" / "b").glob("*.parquet"))
    assert not (tmp_path / "wh" / "raw").exists()
