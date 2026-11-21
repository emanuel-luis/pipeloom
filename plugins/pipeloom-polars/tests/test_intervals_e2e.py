"""Interval incrementals end to end on polars (ADR-0054)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from click.testing import CliRunner

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.fs_table import FsTable
from pipeloom.cli import main
from pipeloom.graph.selectors import Selection
from pipeloom.runtime import intervals
from pipeloom.runtime.executors.process import ChildCommand
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
from click.testing import CliRunner

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


# -- pipeloom backfill (ADR-0054, Backfill) ----------------------------------------------------

NOW = T0 + timedelta(days=9, hours=12)  # inside the 10th day


def day(n: int) -> datetime:
    return T0 + timedelta(days=n)


def backfill(
    root: Path, run_id: str, start: int, end: int, now: datetime = NOW, **kw: Any
) -> tuple[RunOutcome, dict[str, Any]]:
    """`pipeloom backfill -s daily --start day(start) --end day(end)`."""
    clock = FakeClock(now, step=timedelta(0))
    project = Project(ProjectOptions(project_dir=root, target="dev", env={}), clock=clock)
    bf = intervals.Backfill(day(start), day(end), **kw)
    opts = RunOptions(run_id=run_id, workers=1, selection=Selection(("daily",)), backfill=bf)
    outcome = project.run(opts)
    doc = json.loads((root / "target" / "runs" / run_id / "run_results.json").read_bytes())
    assert doc["args"]["backfill"] == bf.to_json()
    (row,) = doc["results"]
    return outcome, row


def starts(row: dict[str, Any]) -> list[int]:
    return [(datetime.fromisoformat(i["start"]) - T0).days for i in row["intervals"]]


def test_a_range_with_one_gap_reruns_only_the_gap(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10)
    assert backfill(tmp_path, "b1", 0, 4)[0].exit_code == 0
    assert backfill(tmp_path, "b2", 6, 10)[0].exit_code == 0
    assert ledger(tmp_path) == [(day(0), day(4)), (day(6), NOW)]
    outcome, row = backfill(tmp_path, "b3", 0, 10)
    assert outcome.exit_code == 0 and starts(row) == [4, 5]
    assert table(tmp_path).height == 20 and ledger(tmp_path) == [(day(0), NOW)]


def test_rerun_missing_after_success_writes_nothing(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10)
    assert backfill(tmp_path, "b1", 2, 5)[0].exit_code == 0
    files = FsTable(tmp_path / "wh" / "main" / "daily", clock=FakeClock()).files()
    outcome, row = backfill(tmp_path, "b2", 2, 5)
    assert outcome.exit_code == 0 and row["intervals"] == [] and row["rows_written"] == 0
    assert FsTable(tmp_path / "wh" / "main" / "daily", clock=FakeClock()).files() == files


def test_bounds_off_a_window_edge_round_outward(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10)
    clock = FakeClock(NOW, step=timedelta(0))
    project = Project(ProjectOptions(project_dir=tmp_path, target="dev", env={}), clock=clock)
    bf = intervals.Backfill(day(2) + timedelta(hours=5), day(4) + timedelta(hours=1))
    outcome = project.run(RunOptions(run_id="b1", selection=Selection(("daily",)), backfill=bf))
    assert outcome.exit_code == 0
    assert ledger(tmp_path) == [(day(2), day(5))]


def test_a_killed_backfill_resumes_only_the_missing_windows(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10, poison=2)
    outcome, row = backfill(tmp_path, "b1", 0, 6)
    assert outcome.exit_code == 1 and starts(row) == [0, 1, 2]
    assert row["intervals"][2]["failed"] is True and "failed" not in row["intervals"][1]
    feed(tmp_path, 10)
    outcome, row = backfill(tmp_path, "b2", 0, 6)
    assert outcome.exit_code == 0 and starts(row) == [2, 3, 4, 5]
    assert ledger(tmp_path) == [(day(0), day(6))] and table(tmp_path).height == 12


def test_on_failure_continue_reports_every_failed_window(tmp_path: Path) -> None:
    setup(tmp_path)
    feed_rows(tmp_path, [day(d) + timedelta(hours=1) for d in range(5)], [1, -1, 1, -1, 1])
    outcome, row = backfill(tmp_path, "b1", 0, 5, on_failure="continue")
    assert outcome.exit_code == 1 and starts(row) == [0, 1, 2, 3, 4]
    assert [i.get("failed", False) for i in row["intervals"]] == [False, True, False, True, False]
    message = row["error"]["message"]
    assert "2 of 5 windows" in message
    assert all(row["intervals"][n]["interval_id"] in message for n in (1, 3))
    assert ledger(tmp_path) == [(day(0), day(1)), (day(2), day(3)), (day(4), day(5))]
    assert table(tmp_path).height == 3


def test_mark_done_then_backfill_writes_zero_windows(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10)
    for run_id in ("m1", "m2"):  # safe to repeat
        outcome, row = backfill(tmp_path, run_id, 0, 8, mark_done=True)
        assert outcome.exit_code == 0 and row["intervals"] == [] and row["rows_written"] == 0
        assert ledger(tmp_path) == [(day(0), day(8))]
    outcome, row = backfill(tmp_path, "b1", 0, 8)
    assert outcome.exit_code == 0 and row["intervals"] == []
    assert not (tmp_path / "wh" / "main" / "daily").exists()
    outcome, row = run(tmp_path, "r1", NOW)  # the scheduled run picks up after the watermark
    assert outcome.exit_code == 0 and starts(row) == [8, 9]


def test_rerun_all_restates_and_resumes_as_missing(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10)
    assert backfill(tmp_path, "b1", 0, 10)[0].exit_code == 0
    outcome, row = backfill(tmp_path, "b2", 2, 4, rerun="all")
    assert outcome.exit_code == 0 and starts(row) == [2, 3]
    assert table(tmp_path).height == 20
    feed(tmp_path, 10, poison=4)
    outcome, row = backfill(tmp_path, "b3", 3, 6, rerun="all")  # killed at day 4
    assert outcome.exit_code == 1 and starts(row) == [3, 4]
    assert ledger(tmp_path) == [(day(0), day(4)), (day(6), NOW)]
    feed(tmp_path, 10)
    outcome, row = backfill(tmp_path, "b4", 0, 10)
    assert outcome.exit_code == 0 and starts(row) == [4, 5]


@pytest.mark.timeout(300)
def test_backfill_reaches_the_child_processes(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10)
    clock = FakeClock(NOW, step=timedelta(0))
    project = Project(ProjectOptions(project_dir=tmp_path, target="dev", env={}), clock=clock)
    bf = intervals.Backfill(day(1), day(3))
    opts = RunOptions(run_id="b1", selection=Selection(("daily",)), backfill=bf, boundary="task")
    outcome = project.run(opts)
    assert outcome.exit_code == 0, [d.message for d in outcome.diagnostics]
    assert ledger(tmp_path) == [(day(1), day(3))]


# -- the backfill command ----------------------------------------------------------------------


def cli(root: Path, *args: str) -> Any:
    return CliRunner().invoke(
        main, [args[0], "--project-dir", str(root), *args[1:]], catch_exceptions=False
    )


def results(root: Path, run_id: str) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(
        (root / "target" / "runs" / run_id / "run_results.json").read_bytes()
    )
    return doc


def test_backfill_command_and_its_flags(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10)
    sel = ("--select", "daily")
    result = cli(tmp_path, "backfill", *sel, "--start", "2026-01-01", "--end", "2026-01-04",
                 "--run-id", "c1")  # fmt: skip
    assert result.exit_code == 0, result.output
    assert results(tmp_path, "c1")["args"]["backfill"] == {
        "start": "2026-01-01T00:00:00+00:00",
        "end": "2026-01-04T00:00:00+00:00",
        "rerun": "missing",
        "on_failure": "stop",
        "mark_done": False,
    }
    assert ledger(tmp_path) == [(day(0), day(3))]

    result = cli(tmp_path, "backfill", *sel, "--start", "2026-01-02T00:00:00+00:00",
                 "--end", "2026-01-03", "--rerun", "all", "--run-id", "c2")  # fmt: skip
    assert result.exit_code == 0, result.output
    assert starts(results(tmp_path, "c2")["results"][0]) == [1]

    feed(tmp_path, 10, poison=5)
    result = cli(tmp_path, "backfill", *sel, "--start", "2026-01-05", "--end", "2026-01-08",
                 "--on-failure", "continue", "--run-id", "c3")  # fmt: skip
    assert result.exit_code == 1
    (row,) = results(tmp_path, "c3")["results"]
    assert [i.get("failed", False) for i in row["intervals"]] == [False, True, False]

    result = cli(tmp_path, "backfill", *sel, "--start", "2026-01-01", "--end", "2026-01-09",
                 "--mark-done", "--run-id", "c4")  # fmt: skip
    assert result.exit_code == 0, result.output
    assert ledger(tmp_path) == [(day(0), day(8))]


def test_backfill_usage_errors(tmp_path: Path) -> None:
    setup(tmp_path, {"models/plain.py": "from pipeloom import model\n\n\n@model\ndef plain(ctx):\n"
                     "    return [{'x': 1}]\n"})  # fmt: skip
    feed(tmp_path, 3)
    rng = ("--start", "2026-01-01", "--end", "2026-01-02")
    assert cli(tmp_path, "backfill", *rng).exit_code == 2  # no --select
    assert cli(tmp_path, "backfill", "-s", "daily", "--start", "2026-01-02",
               "--end", "2026-01-01").exit_code == 2  # fmt: skip
    assert cli(tmp_path, "backfill", "-s", "daily", "--start", "soon",
               "--end", "2026-01-01").exit_code == 2  # fmt: skip
    assert cli(tmp_path, "backfill", "-s", "daily", *rng, "--rerun", "some").exit_code == 2
    assert cli(tmp_path, "backfill", "-s", "daily", *rng, "--on-failure", "x").exit_code == 2
    result = cli(tmp_path, "backfill", "-s", "plain", *rng)
    assert result.exit_code == 2 and "PLX132" in result.output
    result = cli(tmp_path, "backfill", "-s", "daily", "-s", "plain", *rng, "--mark-done")
    assert result.exit_code == 2 and "plain" in result.output
    assert [p.name for p in (tmp_path / "target" / "runs").iterdir()] == ["_invocations"]


def test_hidden_backfill_flag_on_run(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 3)
    bf = intervals.Backfill(day(0), day(2))
    spec = json.dumps(bf.to_json())
    result = cli(tmp_path, "run", "-s", "daily", "--backfill", spec, "--run-id", "h1")
    assert result.exit_code == 0, result.output
    assert ledger(tmp_path) == [(day(0), day(2))]
    assert "--backfill" not in cli(tmp_path, "run", "--help").output
    assert "--backfill" not in cli(tmp_path, "build", "--help").output
    assert cli(tmp_path, "run", "--backfill", "{}").exit_code == 2
    child = ChildCommand(tmp_path, "dev", tmp_path / "m.json", "r", "0" * 64, False, None, (),
                         backfill=bf)  # fmt: skip
    argv = child.argv("u")
    assert json.loads(argv[argv.index("--backfill") + 1]) == bf.to_json()


def test_retry_of_a_restate_runs_only_what_is_missing(tmp_path: Path) -> None:
    setup(tmp_path)
    feed(tmp_path, 10, poison=3)
    result = cli(tmp_path, "backfill", "-s", "daily", "--start", "2026-01-01",
                 "--end", "2026-01-06", "--rerun", "all", "--run-id", "x1")  # fmt: skip
    assert result.exit_code == 1
    feed(tmp_path, 10)
    result = cli(tmp_path, "retry", "--from", "x1", "--run-id", "x2")
    assert result.exit_code == 0, result.output
    doc = results(tmp_path, "x2")
    assert doc["args"]["backfill"]["rerun"] == "missing"
    assert starts(doc["results"][0]) == [3, 4]


def test_retry_of_a_backfill_whose_failed_unit_has_no_interval(tmp_path: Path) -> None:
    after = (
        "from pipeloom import model, ref\n\n\n"
        '@model(inputs={"d": ref("daily")})\n'
        "def after(ctx, d):\n"
        '    if ctx.vars.get("act") == "fail":\n'
        '        raise ValueError("boom")\n'
        "    return d\n"
    )
    setup(tmp_path, {"models/after.py": after})
    feed(tmp_path, 10)
    result = cli(tmp_path, "backfill", "-s", "daily+", "--start", "2026-01-01",
                 "--end", "2026-01-03", "--vars", '{"act": "fail"}', "--run-id", "y1")  # fmt: skip
    assert result.exit_code == 1
    result = cli(tmp_path, "retry", "--from", "y1", "--run-id", "y2", "--vars", '{"act": "ok"}')
    assert result.exit_code == 0, result.output
    doc = results(tmp_path, "y2")
    assert [r["unique_id"].rsplit(".", 1)[-1] for r in doc["results"]] == ["after"]
    assert doc["args"]["backfill"]["rerun"] == "missing"
