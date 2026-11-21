"""Interval incrementals end to end on polars (ADR-0054)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.fs_table import FsTable
from pipeloom.runtime import intervals
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.state.jsondir import JsonDirStore
from pipeloom.state.types import StateKey
from pipeloom.testing.clock import FakeClock

T0 = datetime(2026, 1, 1, tzinfo=UTC)
FILES = {
    "pipeloom.yml": """\
config_version: 1
name: shop
python_paths: [models]
sources:
  raw:
    tables:
      events: {format: parquet, location: raw}
targets:
  dev:
    engine: {kind: polars}
    catalog: {kind: filesystem, root: wh}
    state: {kind: json, path: .pipeloom/state}
    naming: {kind: none}
""",
    "models/daily.py": """\
import polars as pl
import pytest

from pipeloom import model, source


@model(
    inputs={"raw": source("raw", "events")},
    materialized="incremental",
    incremental={
        "input": "raw",
        "interval": {"event_time": "ts", "grain": "day", "begin": "2026-01-01"},
    },
)
def daily(ctx, raw):
    if raw.filter(pl.col("v") < 0).collect().height:
        raise ValueError("poisoned window")
    return raw.with_columns(pl.lit(ctx.interval[0].day).alias("day"))
""",
}


def setup(root: Path, extra: dict[str, str] | None = None) -> None:
    for name, text in {**FILES, **(extra or {})}.items():
        (root / name).parent.mkdir(parents=True, exist_ok=True)
        (root / name).write_bytes(text.encode("utf-8"))


def feed(root: Path, days: int, poison: int | None = None) -> None:
    """Two events a day; day `poison` (0-based) gets a negative value."""
    ts = [T0 + timedelta(days=d, hours=h) for d in range(days) for h in (1, 23)]
    v = [
        -1 if poison is not None and t.date() == (T0 + timedelta(days=poison)).date() else 1
        for t in ts
    ]
    (root / "raw").mkdir(exist_ok=True)
    frame = pl.DataFrame({"ts": ts, "v": v}, schema={"ts": pl.Datetime("us", "UTC"), "v": pl.Int64})
    frame.write_parquet(root / "raw" / "part.parquet")


def run(
    root: Path, run_id: str, now: datetime, *, full_refresh: bool = False, model: str = "daily"
) -> tuple[RunOutcome, dict[str, Any]]:
    clock = FakeClock(now, step=timedelta(0))
    project = Project(ProjectOptions(project_dir=root, target="dev", env={}), clock=clock)
    outcome = project.run(RunOptions(run_id=run_id, workers=1, full_refresh=full_refresh))
    assert (root / "target" / "runs" / run_id).is_dir(), [
        (d.code, d.message) for d in outcome.diagnostics
    ]
    path = root / "target" / "runs" / run_id / "run_results.json"
    rows = json.loads(path.read_bytes())["results"]
    (row,) = [r for r in rows if r["unique_id"].endswith("." + model)]
    return outcome, row


def table(root: Path, name: str = "daily") -> pl.DataFrame:
    files = FsTable(root / "wh" / "main" / name, clock=FakeClock()).files()
    return pl.read_parquet([str(p) for p in files]).sort("ts")


def ledger(root: Path) -> list[tuple[datetime, datetime]]:
    store = JsonDirStore(root / ".pipeloom" / "state")
    try:
        return intervals.load(store, StateKey("shop", "dev", "daily"))
    finally:
        store.close()


def test_a_90_day_run_writes_exactly_90_windows(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 90)
    now = T0 + timedelta(days=89, hours=12)  # inside the 90th day
    outcome, row = run(tmp_path, "r1", now)
    assert outcome.exit_code == 0, [d.message for d in outcome.diagnostics]
    assert len(row["intervals"]) == 90 and row["rows_written"] == 180
    assert (
        row["intervals"][0]["idempotency_token"] == f"r1:daily:{row['intervals'][0]['interval_id']}"
    )
    assert len({i["interval_id"] for i in row["intervals"]}) == 90
    data = table(tmp_path)
    assert data.height == 180 and data.get_column("day").n_unique() == 31
    assert ledger(tmp_path) == [(T0, now)]

    outcome, row = run(tmp_path, "r2", now)  # nothing missing: nothing runs
    assert outcome.exit_code == 0 and row["intervals"] == [] and row["rows_written"] == 0

    later = now + timedelta(days=1)  # the partial day reruns in full, and the next day runs
    feed(tmp_path, 91)
    outcome, row = run(tmp_path, "r3", later)
    assert outcome.exit_code == 0
    assert [i["start"][:10] for i in row["intervals"]] == ["2026-03-31", "2026-04-01"]
    assert table(tmp_path).height == 182


def test_a_failing_window_keeps_the_earlier_ones(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 5, poison=2)
    now = T0 + timedelta(days=4, hours=12)
    outcome, row = run(tmp_path, "r1", now)
    assert outcome.exit_code == 1 and row["status"] == "failed"
    assert len(row["intervals"]) == 3  # the third window failed
    assert ledger(tmp_path) == [(T0, T0 + timedelta(days=2))]
    assert table(tmp_path).height == 4

    feed(tmp_path, 5)
    outcome, row = run(tmp_path, "r2", now)
    assert outcome.exit_code == 0
    assert [i["start"][:10] for i in row["intervals"]] == ["2026-01-03", "2026-01-04", "2026-01-05"]
    assert table(tmp_path).height == 10
    assert ledger(tmp_path) == [(T0, now)]


def head(decorator_args: str, body: str) -> dict[str, str]:
    """`models/daily.py` replaced by an interval model with other options."""
    return {
        "models/daily.py": (
            "import polars as pl\n\nfrom pipeloom import model, source\n\n\n"
            '@model(\n    inputs={"raw": source("raw", "events")},\n'
            '    materialized="incremental",\n'
            f"{decorator_args}"
            ")\n"
            f"def daily(ctx, raw):\n{body}"
        )
    }


def feed_rows(root: Path, ts: list[datetime], v: list[int]) -> None:
    (root / "raw").mkdir(exist_ok=True)
    frame = pl.DataFrame({"ts": ts, "v": v}, schema={"ts": pl.Datetime("us", "UTC"), "v": pl.Int64})
    frame.write_parquet(root / "raw" / "part.parquet")


def test_every_window_quarantines_its_own_rows(tmp_path: Path) -> None:
    args = (
        '    incremental={"input": "raw", "interval": '
        '{"event_time": "ts", "grain": "day", "begin": "2026-01-01"}},\n'
        '    quality={"quarantine": True, "max_drop_ratio": 1.0},\n'
        '    expectations=[{"name": "v_positive", "check": {"range": {"column": "v", "min": 0}},'
        ' "on_violation": "drop"}],\n'
    )
    setup(tmp_path, head(args, "    return raw\n"))
    feed(tmp_path, 3)
    ts = [T0 + timedelta(days=d, hours=h) for d in range(3) for h in (1, 23)]
    feed_rows(tmp_path, ts, [-1 if t.hour == 1 else 1 for t in ts])  # one bad row a window
    outcome, row = run(tmp_path, "r1", T0 + timedelta(days=2, hours=23, minutes=30))
    assert outcome.exit_code == 0, [d.message for d in outcome.diagnostics]
    assert len(row["intervals"]) == 3
    assert row["rows_dropped"] == 3 and row["rows_quarantined"] == 3
    (check,) = row["expectations"]
    assert check["failed"] == 3 and check["passed"] == 3
    assert table(tmp_path, "daily__quarantine").height == 3
    assert table(tmp_path).height == 3


@pytest.mark.timeout(300)  # 231 windows; a window write reads every file of the table
def test_a_retried_full_refresh_never_overwrites_written_windows(tmp_path: Path) -> None:
    """More windows than the fs token log keeps: the first window's token is evicted, yet the
    retry must not overwrite the table with it."""
    args = (
        '    incremental={"input": "raw", "interval": '
        '{"event_time": "ts", "grain": "hour", "begin": "2026-01-01"}},\n'
    )
    body = '    if raw.filter(pl.col("v") < 0).collect().height:\n        raise ValueError("bad")\n    return raw\n'
    setup(tmp_path, head(args, body))
    ts = [T0 + timedelta(hours=h, minutes=30) for h in range(121)]
    now = T0 + timedelta(hours=120, minutes=45)
    feed_rows(tmp_path, ts, [-1 if i == 110 else 1 for i in range(121)])
    outcome, row = run(tmp_path, "r1", now, full_refresh=True)
    assert outcome.exit_code == 1 and len(row["intervals"]) == 111
    assert table(tmp_path).height == 110

    feed_rows(tmp_path, ts, [1] * 121)
    outcome, row = run(tmp_path, "r1", now, full_refresh=True)  # the same run, retried
    assert outcome.exit_code == 0, [d.message for d in outcome.diagnostics]
    assert table(tmp_path).height == 121
    assert table(tmp_path).get_column("ts").to_list() == ts
