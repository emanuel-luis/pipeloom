"""Repair a finished job run: rerun the tasks that did not succeed, and finalize again."""

from __future__ import annotations

from typing import Final

from pipeloom.diagnostics import Report
from pipeloom.secrets.redaction import Redactor
from pipeloom_databricks.client import DatabricksClient, RunInfo
from pipeloom_databricks.deploy import api_errors
from pipeloom_databricks.keys import FINALIZE_KEY
from pipeloom_databricks.run import TERMINAL

REPAIRABLE: Final = frozenset(
    {
        "FAILED",
        "TIMEDOUT",
        "CANCELED",
        "UPSTREAM_FAILED",
        "UPSTREAM_CANCELED",
        "MAXIMUM_CONCURRENT_RUNS_REACHED",
    }
)
"""Task result states a repair runs again. SUCCESS and EXCLUDED tasks keep their result."""


def rerun_set(run: RunInfo) -> list[str]:
    """The tasks to run again, sorted: those in a repairable state, plus finalize when anything
    else is repaired. Empty when nothing failed."""
    failed = {t.task_key for t in run.tasks if t.result_state in REPAIRABLE}
    if not failed:
        return []
    if any(t.task_key == FINALIZE_KEY for t in run.tasks):
        failed.add(FINALIZE_KEY)
    return sorted(failed)


def repair(
    client: DatabricksClient,
    job_run_id: int,
    report: Report,
    *,
    redactor: Redactor | None = None,
) -> int | None:
    """Repair job run `job_run_id` and return the repair id.

    None with no error in `report` means there was nothing to repair; None with PLD014 means the
    run is still active; None with PLD009 means an API call failed.
    """
    with api_errors(report, redactor):
        run = client.get_run(job_run_id)
        if run.life_cycle_state not in TERMINAL:
            report.add(
                "PLD014",
                f"run {job_run_id} is still {run.life_cycle_state}",
                hint="wait for it to end, or cancel it, then repair",
                context={"job_run_id": job_run_id, "state": run.life_cycle_state},
            )
            return None
        rerun = rerun_set(run)
        if not rerun:
            return None
        return client.repair_run(
            job_run_id,
            rerun_tasks=rerun,
            latest_repair_id=run.latest_repair_id,
            # Without them the repair resolves {{job.parameters.*}} to the defaults, so an empty
            # pipeloom_run_id would become dbx-<job run id> and start a new pipeloom run.
            job_parameters=run.job_parameters,
        )
    return None
