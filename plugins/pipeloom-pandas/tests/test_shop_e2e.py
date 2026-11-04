"""The pandas smoke project end to end, with both executors.

The project is `tests/fixtures/smoke-pandas`: csv sources, Python models (one on Narwhals frames),
an ephemeral model and an append table, on the filesystem catalog and json state.
"""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.fs_table import FsTable
from pipeloom.events.bus import EventBus
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink

SMOKE = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "smoke-pandas"


def project(tmp_path: Path, boundary: str) -> Path:
    shutil.copytree(
        SMOKE, tmp_path, ignore=shutil.ignore_patterns("target", ".pipeloom"), dirs_exist_ok=True
    )
    yml = tmp_path / "pipeloom.yml"
    text = yml.read_text(encoding="utf-8").replace(
        "python_paths:", f"boundary: {boundary}\npython_paths:"
    )
    yml.write_text(text, encoding="utf-8", newline="\n")
    return tmp_path


def run(root: Path, run_id: str) -> tuple[RunOutcome, RecordingSink]:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    sink = RecordingSink()
    proj = Project(
        ProjectOptions(project_dir=root, target="pandas", env={"PIPELOOM_USER": "tester"}),
        clock=clock,
        invocation_id="inv",
    )
    with EventBus([(sink, 5)], clock=clock, invocation_id="inv") as bus:
        return proj.run(RunOptions(run_id=run_id, workers=2), bus), sink


def table(root: Path, name: str) -> list[dict[str, Any]]:
    t = FsTable(root / "target" / "warehouse" / "main" / name, clock=FakeClock())
    df = pd.concat([pd.read_parquet(f) for f in t.files()], ignore_index=True)
    df = df.sort_values(list(df.columns)).reset_index(drop=True)
    return [
        {k: v.item() if hasattr(v, "item") else v for k, v in r.items()}
        for r in df.to_dict("records")
    ]


def snapshot(root: Path) -> dict[str, bytes]:
    wh = root / "target" / "warehouse"
    return {
        p.relative_to(wh).as_posix(): p.read_bytes() for p in sorted(wh.rglob("*")) if p.is_file()
    }


@pytest.mark.parametrize(("boundary", "executor"), [("process", "thread"), ("task", "process")])
def test_smoke_runs_and_a_rerun_with_the_same_run_id_writes_nothing(
    tmp_path: Path, boundary: str, executor: str
) -> None:
    root = project(tmp_path, boundary)
    outcome, sink = run(root, "r1")
    assert (outcome.status, outcome.exit_code) == ("succeeded", 0), outcome.diagnostics
    doc = json.loads((root / "target" / "runs" / "r1" / "run_results.json").read_bytes())
    assert {r["executor"] for r in doc["results"]} == {executor}
    assert {r["engine"] for r in doc["results"]} == {"pandas"}
    assert table(root, "stg_customers") == [
        {"id": 1, "email": "a@x.com"},
        {"id": 2, "email": "b@x.com"},
    ]
    assert table(root, "fct_orders") == [
        {"id": 10, "customer_id": 1, "email": "a@x.com", "amount": 5.0},
        {"id": 11, "customer_id": 2, "email": "b@x.com", "amount": 7.5},
        {"id": 12, "customer_id": 1, "email": "a@x.com", "amount": 2.5},
    ]
    assert table(root, "rpt_revenue") == [
        {"customer_id": 1, "revenue": 7.5},
        {"customer_id": 2, "revenue": 7.5},
    ]
    assert not [e for e in sink.events if e.type == "unit.failed"]

    before = snapshot(root)
    again, _ = run(root, "r1")
    assert again.exit_code == 0
    assert snapshot(root) == before  # token replay: no new part file, commits untouched
