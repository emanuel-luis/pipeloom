"""What the Jobs API knows about a run's tasks, as the unit states core finalize accepts.

A unit whose task died (a lost cluster, a timeout, a cancel) wrote no attempt file. The finalize
task asks the Jobs API how each task ended and hands the answer to `pipeloom finalize
--unit-states`, so the run fails (or is cancelled) instead of looking like a success.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping

from pipeloom.runtime.finalize_run import UNIT_STATES_SCHEMA, ExternalUnitState
from pipeloom_databricks.client import DatabricksClient, RunInfo, TaskRunInfo


def task_states(client: DatabricksClient, job_run_id: int) -> Mapping[str, TaskRunInfo]:
    """The tasks of a job run by task key (the latest attempt of each)."""
    return {t.task_key: t for t in client.get_run(job_run_id).tasks}


def unit_states(
    key_map: Mapping[str, str], run: RunInfo, with_attempts: Collection[str]
) -> dict[str, ExternalUnitState]:
    """The external state of every unit that has no attempt file.

    `key_map` is unit id -> task key. EXCLUDED is `excluded`; UPSTREAM_FAILED and
    UPSTREAM_CANCELED are `skipped` (upstream_failed); CANCELED and TIMEDOUT are `cancelled`; FAILED
    is `failed` with PLD011 and the termination message. Any other state (success, still running,
    unknown task) gives no entry, so core finalize counts the unit as not run.
    """
    tasks = {t.task_key: t for t in run.tasks}
    out: dict[str, ExternalUnitState] = {}
    for unit, key in sorted(key_map.items()):
        task = tasks.get(key)
        if unit in with_attempts or task is None:
            continue
        ids = {
            "databricks_task_key": task.task_key,
            "databricks_task_run_id": str(task.run_id),
        }
        match task.result_state:
            case "EXCLUDED":
                out[unit] = ExternalUnitState("excluded")
            case "UPSTREAM_FAILED" | "UPSTREAM_CANCELED":
                out[unit] = ExternalUnitState("skipped", orchestrator_ids=ids)
            case "CANCELED" | "TIMEDOUT":
                out[unit] = ExternalUnitState("cancelled", orchestrator_ids=ids)
            case "FAILED":
                reason = task.message or task.termination_code or "no reason given"
                out[unit] = ExternalUnitState(
                    "failed",
                    code="PLD011",
                    message=f"task {task.task_key} ended without a unit result: {reason}",
                    orchestrator_ids=ids,
                )
    return out


def unit_states_json(states: Mapping[str, ExternalUnitState]) -> bytes:
    """The `pipeloom.unit_states/1` file for `states`."""
    units: dict[str, dict[str, object]] = {}
    for unit, state in sorted(states.items()):
        doc: dict[str, object] = {"status": state.status}
        for name in ("reason", "code", "message"):
            if getattr(state, name) is not None:
                doc[name] = getattr(state, name)
        if state.orchestrator_ids:
            doc["orchestrator_ids"] = dict(state.orchestrator_ids)
        units[unit] = doc
    doc_text = json.dumps({"schema": UNIT_STATES_SCHEMA, "units": units}, ensure_ascii=True)
    return (doc_text + "\n").encode("ascii")
