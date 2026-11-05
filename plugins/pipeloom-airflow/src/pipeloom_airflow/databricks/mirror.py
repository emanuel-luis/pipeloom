"""What a Databricks job run says about one of its tasks, as an Airflow task outcome. Pure."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

from pydantic import JsonValue

from pipeloom_databricks.api import TERMINAL, RunInfo, TaskRunInfo

SUCCEEDED: Final = frozenset({"SUCCESS"})
SKIPPED: Final = frozenset({"SKIPPED", "EXCLUDED", "UPSTREAM_FAILED", "UPSTREAM_CANCELED"})
FAILED: Final = frozenset(
    {"FAILED", "TIMEDOUT", "CANCELED", "INTERNAL_ERROR", "MAXIMUM_CONCURRENT_RUNS_REACHED"}
)
SNAPSHOT_VERSION: Final = 1


@dataclass(frozen=True)
class Verdict:
    outcome: Literal["success", "failed", "skipped"]
    attempt: int
    message: str | None


def latest_attempt(run: RunInfo, task_key: str) -> TaskRunInfo | None:
    """The entry of `task_key` with the highest attempt; retries may be separate entries."""
    entries = [t for t in run.tasks if t.task_key == task_key]
    return max(entries, key=lambda t: t.attempt) if entries else None


def task_verdict(
    run: RunInfo,
    task_key: str,
    *,
    baseline_attempt: int,
    max_attempts: int,
    ended: bool | None = None,
) -> Verdict | None:
    """The outcome of task `task_key`, or None while it is still open.

    `ended` says whether the run is over for this launch; the default is "its life cycle state is
    terminal". After a repair, pass `run_ended(...)`: a terminal run that still shows the state
    from before the repair is not over yet.

    While the run is active, an attempt below `baseline_attempt` (one a repair is rerunning) is
    ignored, and a failed attempt counts only once it is the last one Databricks will make
    (`attempt >= max_attempts - 1`). A terminal run decides every task: a task it does not list
    was skipped, and a state no rule names is a failure.
    """
    terminal = run.life_cycle_state in TERMINAL if ended is None else ended
    task = latest_attempt(run, task_key)
    if task is None:
        return Verdict("skipped", 0, f"task {task_key} did not run") if terminal else None
    if not terminal and task.attempt < baseline_attempt:
        return None
    state = task.result_state or ""
    message = task.message
    if state in SUCCEEDED:
        return Verdict("success", task.attempt, message)
    if state in SKIPPED or task.life_cycle_state == "SKIPPED":
        return Verdict("skipped", task.attempt, message or f"task {task_key} ended as {state}")
    if state in FAILED and (terminal or task.attempt >= max_attempts - 1):
        reason = message or task.termination_code or state
        return Verdict("failed", task.attempt, f"task {task_key} ended as {state}: {reason}")
    if terminal:
        shown = f"{task.life_cycle_state}/{state}" if state else task.life_cycle_state
        return Verdict("failed", task.attempt, f"task {task_key} ended as {shown}")
    return None


def run_ended(run: RunInfo, repair_id: int | None, seen_active: bool) -> bool:
    """The run is over, and after a repair, over for that repair (as `wait_for_run` decides):
    the run reports the repair or a later one, and its history entry is terminal or the run was
    seen active since the repair started."""
    if run.life_cycle_state not in TERMINAL:
        return False
    if repair_id is None:
        return True  # no repair: terminal is over
    latest = run.latest_repair_id or 0
    ours_done = run.repair_states.get(repair_id) in TERMINAL
    return latest > repair_id or (latest == repair_id and (ours_done or seen_active))


def snapshot(run: RunInfo) -> dict[str, JsonValue]:
    """The run as plain JSON for an XCom: its states and each task's latest attempt."""
    latest = [latest_attempt(run, key) for key in sorted({t.task_key for t in run.tasks})]
    tasks: dict[str, JsonValue] = {
        t.task_key: {
            "run_id": t.run_id,
            "attempt": t.attempt,
            "life_cycle_state": t.life_cycle_state,
            "result_state": t.result_state,
            "termination_code": t.termination_code,
            "message": t.message,
        }
        for t in latest
        if t is not None
    }
    return {
        "v": SNAPSHOT_VERSION,
        "run_id": run.run_id,
        "job_id": run.job_id,
        "life_cycle_state": run.life_cycle_state,
        "result_state": run.result_state,
        "latest_repair_id": run.latest_repair_id,
        "run_page_url": run.run_page_url,
        "tasks": tasks,
    }


def run_from_snapshot(doc: Mapping[str, Any]) -> RunInfo:
    """The `RunInfo` a snapshot was made from (without parameters and repair history)."""
    tasks = tuple(
        TaskRunInfo(
            task_key=key,
            run_id=int(t["run_id"]),
            attempt=int(t["attempt"]),
            life_cycle_state=str(t["life_cycle_state"]),
            result_state=t.get("result_state"),
            termination_code=t.get("termination_code"),
            message=t.get("message"),
        )
        for key, t in sorted(dict(doc.get("tasks") or {}).items())
    )
    return RunInfo(
        run_id=int(doc["run_id"]),
        job_id=doc.get("job_id"),
        life_cycle_state=str(doc["life_cycle_state"]),
        result_state=doc.get("result_state"),
        tasks=tasks,
        latest_repair_id=doc.get("latest_repair_id"),
        run_page_url=doc.get("run_page_url"),
    )
