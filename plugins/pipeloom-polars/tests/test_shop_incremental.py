"""An incremental merge model end to end on polars, with both executors."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.fs_table import FsTable
from pipeloom.events.bus import EventBus
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink

CSV = "id,seq,v\n"


def project(tmp_path: Path, boundary: str) -> Path:
    files = {
        "pipeloom.yml": (
            f"config_version: 1\nname: shop\nboundary: {boundary}\npython_paths: [src]\n"
            "targets:\n  polars:\n    engine: {kind: polars}\n"
            "    catalog: {kind: filesystem, root: target/warehouse}\n"
            "    state: {kind: json, path: .pipeloom/state}\n    naming: {kind: none}\n"
        ),
        "models/sources.yml": (
            "sources:\n  - {name: raw_events, format: csv, location: raw/events.csv}\n"
        ),
        "models/m.yml": (
            "models:\n  - name: events\n    materialized: incremental\n"
            "    sql: select id, seq, v from raw_events\n"
            "    inputs: {raw_events: {source: raw_events}}\n"
            "    incremental: {input: raw_events, column: seq}\n"
            "    write: {strategy: merge, unique_key: [id]}\n"
        ),
    }
    for rel, text in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_bytes(text.encode("utf-8"))
    return tmp_path


def feed(root: Path, rows: list[tuple[int, int, str]]) -> None:
    text = CSV + "".join(f"{i},{s},{v}\n" for i, s, v in rows)
    (root / "raw").mkdir(exist_ok=True)
    (root / "raw" / "events.csv").write_bytes(text.encode("utf-8"))


def run(root: Path, run_id: str) -> tuple[RunOutcome, RecordingSink, dict[str, Any]]:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    sink = RecordingSink()
    proj = Project(
        ProjectOptions(project_dir=root, target="polars", env={}), clock=clock, invocation_id="inv"
    )
    with EventBus([(sink, 5)], clock=clock, invocation_id="inv") as bus:
        outcome = proj.run(RunOptions(run_id=run_id, workers=2), bus)
    path = root / "target" / "runs" / run_id / "run_results.json"
    (row,) = json.loads(path.read_bytes())["results"]
    return outcome, sink, row


def table(root: Path) -> list[dict[str, Any]]:
    t = FsTable(root / "target" / "warehouse" / "main" / "events", clock=FakeClock())
    return pl.scan_parquet([str(f) for f in t.files()]).collect().sort("id").to_dicts()


def snapshot(root: Path) -> dict[str, bytes]:
    wh = root / "target" / "warehouse"
    return {
        p.relative_to(wh).as_posix(): p.read_bytes() for p in sorted(wh.rglob("*")) if p.is_file()
    }


@pytest.mark.parametrize(("boundary", "executor"), [("process", "thread"), ("task", "process")])
def test_incremental_merge_runs_on_both_executors(
    tmp_path: Path, boundary: str, executor: str
) -> None:
    root = project(tmp_path, boundary)
    feed(root, [(1, 1, "a"), (2, 2, "b")])
    outcome, _, row = run(root, "r1")
    assert outcome.exit_code == 0, outcome.diagnostics
    assert row["executor"] == executor and row["is_incremental"] is False
    assert row["watermark_after"] == 2
    assert [(r["id"], r["v"]) for r in table(root)] == [(1, "a"), (2, "b")]

    feed(root, [(1, 1, "a"), (2, 2, "b"), (2, 3, "B"), (3, 4, "c")])
    outcome, sink, row = run(root, "r2")
    assert outcome.exit_code == 0, outcome.diagnostics
    assert row["is_incremental"] is True and row["strategy"] == "merge"
    assert (row["watermark_before"], row["watermark_after"]) == (2, 4)
    assert (row["rows_inserted"], row["rows_updated"]) == (1, 1)
    assert [(r["id"], r["v"]) for r in table(root)] == [(1, "a"), (2, "B"), (3, "c")]
    assert [e.data["after"] for e in sink.events if e.type == "state.watermark_committed"] == [4]

    before = snapshot(root)
    feed(root, [(9, 9, "z")])  # new input, but the same run id: the token replays the old batch
    again, _, row = run(root, "r2")
    assert again.exit_code == 0 and snapshot(root) == before
    assert row["watermark_after"] == 4
