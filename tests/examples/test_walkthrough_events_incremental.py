"""The events_incremental walkthrough, end to end on DuckDB and polars.

Day 1, then day 2 with late events and a resent one, a rerun with no new data, and a full
refresh. docs/examples/events_incremental.md quotes these numbers.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if not (EXAMPLES / "events_incremental").is_dir():  # the sdist ships tests/ without examples/
    pytest.skip("examples/ is not in this source tree", allow_module_level=True)

DAY1_MAX = "2026-03-01T23:50:00"
DAY2_MAX = "2026-03-02T14:00:00"


def silver_ids(root: Path, engine: str) -> set[str]:
    if engine == "duckdb":
        import duckdb

        con = duckdb.connect(str(root / "target/duckdb/events.duckdb"), read_only=True)
        try:
            rows = con.execute("select event_id from web.events_clean").fetchall()
        finally:
            con.close()
        return {r[0] for r in rows}
    import polars as pl

    parts = sorted((root / "target/warehouse/web/events_clean").glob("*.parquet"))
    return set(pl.read_parquet(parts)["event_id"].to_list())


class Walkthrough:
    def __init__(self, root: Path, engine: str) -> None:
        self.root, self.engine = root, engine

    def run(self, *flags: str) -> dict[str, dict[str, Any]]:
        argv = ["run", "--project-dir", str(self.root), "--target", self.engine, *flags]
        result = CliRunner().invoke(main, argv, env={})
        assert result.exit_code == 0, result.stderr
        doc = json.loads((self.root / "target/run_results.json").read_text(encoding="utf-8"))
        return {r["unit_id"]: r for r in doc["results"]}

    def land(self, day: str) -> None:
        for f in (self.root / "data" / day).glob("*.csv"):
            shutil.copy(f, self.root / "raw/events" / f.name)


@pytest.mark.parametrize("engine", ["duckdb", "polars"])
def test_walkthrough(engine: str, tmp_path: Path) -> None:
    root = tmp_path / "events_incremental"
    shutil.copytree(
        EXAMPLES / "events_incremental",
        root,
        ignore=shutil.ignore_patterns("target", ".pipeloom", "raw", "__pycache__"),
    )
    (root / "raw/events").mkdir(parents=True)
    w = Walkthrough(root, engine)

    # Day 1: everything is new and the watermark is set to the latest event.
    w.land("day1")
    res = w.run()
    assert res["events"]["rows_written"] == 12
    silver = res["events_clean"]
    assert silver["is_incremental"] is False
    assert silver["rows_written"] == 12
    assert (silver["watermark_before"], silver["watermark_after"]) == (None, DAY1_MAX)
    assert res["daily_events"]["rows_written"] == 1
    assert len(silver_ids(root, engine)) == 12

    # Day 2: 3 late events inside the 2h lookback, 1 resent event, 1 event too late for the
    # window, and 5 new events. Only the 8 new rows are written.
    w.land("day2")
    res = w.run()
    assert res["events"]["rows_written"] == 22
    silver = res["events_clean"]
    assert silver["is_incremental"] is True
    assert (silver["watermark_before"], silver["watermark_after"]) == (DAY1_MAX, DAY2_MAX)
    assert silver["rows_written"] == 8
    assert silver["rows_inserted"] == 8
    assert not silver["rows_updated"]  # the resent e011 matched and was left alone
    ids = silver_ids(root, engine)
    assert len(ids) == 20
    assert {"e013", "e014", "e015"} <= ids  # late, inside the window: landed
    assert "e016" not in ids  # late, outside the window: missed by the incremental run
    assert res["daily_events"]["rows_written"] == 2

    # Rerun with no new data: the window is read again, nothing changes, nothing is written.
    res = w.run()
    silver = res["events_clean"]
    assert silver["is_incremental"] is True
    assert (silver["watermark_before"], silver["watermark_after"]) == (DAY2_MAX, DAY2_MAX)
    assert silver["rows_written"] == 0
    assert silver_ids(root, engine) == ids

    # Full refresh: reads everything, rebuilds silver and picks up the event the window missed.
    res = w.run("--full-refresh")
    silver = res["events_clean"]
    assert silver["is_incremental"] is False
    assert silver["watermark_before"] is None
    assert silver["watermark_after"] == DAY2_MAX
    assert silver["rows_written"] == 21
    assert silver_ids(root, engine) == ids | {"e016"}
