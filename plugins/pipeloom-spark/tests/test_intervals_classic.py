"""Interval incrementals end to end on Spark and Delta (ADR-0054, ADR-0032 amendment 1.3)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from tests.conftest import make_project

from pipeloom.api import Project, ProjectOptions
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock

pytestmark = [pytest.mark.spark, pytest.mark.timeout(1800)]

T0 = datetime(2026, 1, 1, tzinfo=UTC)
DAYS = 90
FILES = {
    "pipeloom.yml": (
        "config_version: 1\nname: shop\n"
        "targets:\n"
        "  spark:\n"
        "    engine: {kind: spark}\n"
        "    catalog: {kind: delta_path, root: target/warehouse}\n"
        "    state: {kind: table, path: .pipeloom/state}\n"
        "    naming: {kind: none}\n"
    ),
    "models/sources.yml": "sources:\n  - {name: raw_events, format: parquet, location: raw}\n",
    "models/models.yml": (
        "models:\n"
        "  - name: daily\n"
        "    materialized: incremental\n"
        "    sql: select ts, v from raw\n"
        "    inputs: {raw: {source: raw_events}}\n"
        "    incremental:\n"
        "      input: raw\n"
        "      interval: {event_time: ts, grain: day, begin: '2026-01-01'}\n"
    ),
}


def run(root: Path, run_id: str, now: datetime) -> tuple[RunOutcome, dict[str, Any]]:
    clock = FakeClock(now, step=timedelta(0))
    project = Project(ProjectOptions(project_dir=root, target="spark", env={}), clock=clock)
    outcome = project.run(RunOptions(run_id=run_id, workers=1))
    doc = json.loads((root / "target" / "runs" / run_id / "run_results.json").read_bytes())
    (row,) = doc["results"]
    return outcome, row


def _day(n: int) -> str:
    return (T0 + timedelta(days=n)).date().isoformat()


def commits(root: Path) -> list[str]:
    return sorted(p.as_posix() for p in (root / "target").rglob("_delta_log/*.json"))


def test_a_90_day_run_writes_90_windows_and_a_rerun_writes_nothing(
    classic_spark: Any, tmp_path: Path
) -> None:
    root = make_project(tmp_path, FILES)
    ts = [T0 + timedelta(days=d, hours=h) for d in range(DAYS) for h in (1, 23)]
    raw = classic_spark.createDataFrame([(t, 1) for t in ts], "ts TIMESTAMP, v BIGINT")
    raw.write.parquet((root / "raw").as_posix())
    now = T0 + timedelta(days=DAYS - 1, hours=12)  # inside the last day

    outcome, row = run(root, "r1", now)
    assert outcome.exit_code == 0, [d.message for d in outcome.diagnostics]
    assert len({i["interval_id"] for i in row["intervals"]}) == len(row["intervals"]) == DAYS
    assert row["rows_written"] == 2 * DAYS
    (place,) = [p for p in (root / "target" / "warehouse").rglob("daily") if p.is_dir()]
    table = classic_spark.read.format("delta").load(place.as_posix())
    assert table.count() == 2 * DAYS

    before = commits(root)
    outcome, row = run(root, "r1", now)  # the same run again: the ledger covers every window
    assert outcome.exit_code == 0
    assert row["intervals"] == [] and row["rows_written"] == 0
    assert commits(root) == before

    # A day later the partial day reruns in full: it replaces its rows, never duplicates them.
    outcome, row = run(root, "r2", now + timedelta(days=1))
    assert outcome.exit_code == 0
    assert [i["start"][:10] for i in row["intervals"]] == [_day(DAYS - 1), _day(DAYS)]
    assert table.count() == 2 * DAYS
