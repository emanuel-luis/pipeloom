"""An incremental merge model end to end on pandas."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

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
            "targets:\n  pandas:\n    engine: {kind: pandas}\n"
            "    catalog: {kind: filesystem, root: target/warehouse}\n"
            "    state: {kind: json, path: .pipeloom/state}\n    naming: {kind: none}\n"
        ),
        "models/sources.yml": (
            "sources:\n  - {name: raw_events, format: csv, location: raw/events.csv}\n"
        ),
        "models/m.yml": (
            "models:\n  - name: events\n    materialized: incremental\n"
            "    python: inc_pd.models:events\n"
            "    inputs: {raw_events: {source: raw_events}}\n"
            "    incremental: {input: raw_events, column: seq}\n"
            "    write: {strategy: merge, unique_key: [id]}\n"
        ),
    }
    files["src/inc_pd/__init__.py"] = ""
    files["src/inc_pd/models.py"] = "def events(ctx, raw_events):\n    return raw_events\n"
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
        ProjectOptions(project_dir=root, target="pandas", env={}), clock=clock, invocation_id="inv"
    )
    with EventBus([(sink, 5)], clock=clock, invocation_id="inv") as bus:
        outcome = proj.run(RunOptions(run_id=run_id, workers=2), bus)
    path = root / "target" / "runs" / run_id / "run_results.json"
    (row,) = json.loads(path.read_bytes())["results"]
    return outcome, sink, row


def table(root: Path) -> list[dict[str, Any]]:
    t = FsTable(root / "target" / "warehouse" / "main" / "events", clock=FakeClock())
    df = pd.concat([pd.read_parquet(f) for f in t.files()], ignore_index=True)
    return df.sort_values("id").to_dict("records")  # type: ignore[no-any-return]


def snapshot(root: Path) -> dict[str, bytes]:
    wh = root / "target" / "warehouse"
    return {
        p.relative_to(wh).as_posix(): p.read_bytes() for p in sorted(wh.rglob("*")) if p.is_file()
    }


def test_incremental_merge_runs_on_pandas(tmp_path: Path) -> None:
    root = project(tmp_path, "process")
    feed(root, [(1, 1, "a"), (2, 2, "b")])
    outcome, _, row = run(root, "r1")
    assert outcome.exit_code == 0, outcome.diagnostics
    assert row["is_incremental"] is False
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

    feed(root, [(1, 1, "a"), (2, 2, "b"), (2, 3, "B"), (3, 4, "c")])
    outcome, _, row = run(root, "r3")  # nothing new: an empty batch keeps the watermark
    assert outcome.exit_code == 0, outcome.diagnostics
    assert row["watermark_before"] == row["watermark_after"] == 4 and row["rows_written"] == 0
    assert len(table(root)) == 3
