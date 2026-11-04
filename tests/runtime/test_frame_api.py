"""frame_api: narwhals hands models Narwhals frames and converts the result back."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project

PIPELOOM = """config_version: 1
name: p
python_paths: [src]
targets:
  ci:
    engine: {kind: ENGINE}
    catalog: {kind: CATALOG}
    state: {kind: json, path: state}
    naming: {kind: none}
"""
MODELS = """models:
  - {name: native, python: "m:native", inputs: {raw: {source: raw}}}
  - {name: portable, python: "m:portable", frame_api: narwhals, inputs: {raw: {source: raw}}}
  - name: leaves_native
    python: "m:leaves_native"
    frame_api: narwhals
    inputs: {raw: {source: raw}}
"""
SOURCES = "sources:\n  - {name: raw, format: csv, location: raw.csv}\n"
SRC = """import narwhals.stable.v2 as nw
import polars as pl


def native(ctx, raw):
    assert isinstance(raw, pl.LazyFrame), type(raw)
    return raw


def portable(ctx, raw):
    assert isinstance(raw, nw.LazyFrame), type(raw)
    return raw.with_columns((nw.col("x") * 2).alias("y"))


def leaves_native(ctx, raw):
    return nw.to_native(raw)  # a native frame from a narwhals model is accepted
"""


def run(root: Path) -> RunOutcome:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    project = Project(ProjectOptions(project_dir=root, target="ci"), clock=clock)
    with EventBus([], clock=clock, invocation_id="inv") as bus:
        return project.run(RunOptions(run_id="r1"), bus)


def test_models_get_narwhals_frames_on_polars(tmp_path: Path) -> None:
    pytest.importorskip("pipeloom_polars")
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": PIPELOOM.replace("ENGINE", "polars").replace("CATALOG", "filesystem"),
            "models/m.yml": MODELS,
            "models/s.yml": SOURCES,
            "src/m.py": SRC,
            "raw.csv": "x\n1\n2\n",
        },
    )
    outcome = run(root)
    assert outcome.exit_code == 0, outcome.diagnostics
    doc = json.loads((root / "target" / "runs" / "r1" / "run_results.json").read_bytes())
    assert [(r["unit_id"], r["status"], r["rows_written"]) for r in doc["results"]] == [
        ("leaves_native", "succeeded", 2),
        ("native", "succeeded", 2),
        ("portable", "succeeded", 2),
    ]


def test_narwhals_on_a_non_portable_engine_is_plc508(tmp_path: Path) -> None:
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": PIPELOOM.replace("ENGINE", "memory").replace("CATALOG", "memory"),
            "models/m.yml": "models:\n  - {name: a, python: 'm:a', frame_api: narwhals}\n",
            "src/m.py": "def a(ctx):\n    return ()\n",
        },
    )
    outcome = run(root)
    assert outcome.exit_code == 1
    assert [d.code for d in outcome.diagnostics if d.severity == "error"] == ["PLC508"]
