"""Task verdicts, the end of a run after a repair, and the snapshot round trip."""

from __future__ import annotations

import json

import pytest

from pipeloom_airflow.databricks.mirror import (
    Verdict,
    run_ended,
    run_from_snapshot,
    snapshot,
    task_verdict,
)
from pipeloom_databricks.api import RunInfo, TaskRunInfo


def run_of(life: str, *tasks: TaskRunInfo, **kw: object) -> RunInfo:
    return RunInfo(7, 3, life, None, tasks=tasks, **kw)  # type: ignore[arg-type]


def task(result: str | None, attempt: int = 0, life: str = "TERMINATED") -> TaskRunInfo:
    return TaskRunInfo("k", 11, attempt, life, result, "CODE", "why")


@pytest.mark.parametrize(
    ("life", "t", "baseline", "max_attempts", "expected"),
    [
        ("RUNNING", task("SUCCESS"), 0, 1, "success"),
        ("RUNNING", task("EXCLUDED"), 0, 1, "skipped"),
        ("RUNNING", task("UPSTREAM_FAILED"), 0, 1, "skipped"),
        ("RUNNING", task(None, life="SKIPPED"), 0, 1, "skipped"),
        ("RUNNING", task("FAILED"), 0, 1, "failed"),
        # a failed attempt Databricks will retry is not final yet
        ("RUNNING", task("FAILED", 0), 0, 2, None),
        ("RUNNING", task("TIMEDOUT", 1), 0, 2, "failed"),
        # a terminal run decides
        ("TERMINATED", task("FAILED", 0), 0, 2, "failed"),
        ("TERMINATED", task(None, life="INTERNAL_ERROR"), 0, 1, "failed"),
        ("RUNNING", task(None, life="RUNNING"), 0, 1, None),
        # after a repair, the stale attempt below the baseline is ignored while the run is active
        ("RUNNING", task("FAILED", 0), 1, 1, None),
        ("RUNNING", task("SUCCESS", 1), 1, 1, "success"),
    ],
)
def test_verdicts(
    life: str, t: TaskRunInfo, baseline: int, max_attempts: int, expected: str | None
) -> None:
    got = task_verdict(run_of(life, t), "k", baseline_attempt=baseline, max_attempts=max_attempts)
    assert (got.outcome if got else None) == expected


def test_a_failure_says_why_and_a_missing_task_of_an_ended_run_was_skipped() -> None:
    got = task_verdict(
        run_of("TERMINATED", task("FAILED")), "k", baseline_attempt=0, max_attempts=1
    )
    assert got == Verdict("failed", 0, "task k ended as FAILED: why")
    assert task_verdict(run_of("RUNNING"), "x", baseline_attempt=0, max_attempts=1) is None
    gone = task_verdict(run_of("TERMINATED"), "x", baseline_attempt=0, max_attempts=1)
    assert gone is not None and gone.outcome == "skipped"


def test_the_entry_with_the_highest_attempt_is_the_latest_in_any_order() -> None:
    failed, retried = task("FAILED", 0), task("SUCCESS", 1)
    for entries in ((failed, retried), (retried, failed)):
        run = run_of("RUNNING", *entries)
        verdict = task_verdict(run, "k", baseline_attempt=0, max_attempts=2)
        assert verdict is not None and (verdict.outcome, verdict.attempt) == ("success", 1)
        assert snapshot(run)["tasks"]["k"]["attempt"] == 1  # type: ignore[index]


def test_a_terminal_run_from_before_the_repair_is_not_a_verdict() -> None:
    stale = run_of("TERMINATED", task("FAILED", 0), latest_repair_id=5)
    assert not run_ended(stale, 5, False)
    # without the repair in mind the old failure would be final; with it, the baseline applies
    assert task_verdict(stale, "k", baseline_attempt=0, max_attempts=1) is not None
    ended = run_ended(stale, 5, False)
    assert task_verdict(stale, "k", baseline_attempt=1, max_attempts=1, ended=ended) is None


def test_run_ended_waits_for_the_repair() -> None:
    assert not run_ended(run_of("RUNNING"), None, False)
    assert run_ended(run_of("TERMINATED"), None, False)
    stale = run_of("TERMINATED", latest_repair_id=4)
    assert not run_ended(stale, 5, False)
    shown = run_of("TERMINATED", latest_repair_id=5)
    assert not run_ended(shown, 5, False) and run_ended(shown, 5, True)
    assert run_ended(
        run_of("TERMINATED", latest_repair_id=5, repair_states={5: "TERMINATED"}), 5, False
    )
    assert run_ended(run_of("TERMINATED", latest_repair_id=6), 5, False)


def test_snapshot_is_json_and_round_trips() -> None:
    run = run_of("TERMINATED", task("SUCCESS"), run_page_url="https://x/run/7")
    doc = snapshot(run)
    back = run_from_snapshot(json.loads(json.dumps(doc)))
    assert back.tasks == run.tasks and back.run_page_url == run.run_page_url
    assert (back.run_id, back.job_id, back.life_cycle_state) == (7, 3, "TERMINATED")
