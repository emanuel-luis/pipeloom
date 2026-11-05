"""Start, continue or repair the Databricks job run of one DAG run. Pure: the client is handed in,
so everything here runs on `FakeDatabricks` in tests."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from pydantic import JsonValue

from pipeloom.diagnostics import Report
from pipeloom.graph.selectors import Selection
from pipeloom.plan.model import Plan
from pipeloom_databricks.api import (
    TERMINAL,
    DatabricksClient,
    Runtime,
    SelectionError,
    api_errors,
    find_job,
    find_run,
    job_name,
    repair,
    rerun_set,
    run_job,
)

LAUNCH_VERSION = 1


@dataclass(frozen=True)
class LaunchResult:
    run_id: str
    job_id: int
    job_run_id: int
    run_page_url: str | None
    repair_id: int | None
    baseline: Mapping[str, int] = field(default_factory=dict)
    """Task key -> the first attempt number a repair makes; older attempts are stale."""
    action: Literal["started", "attached", "repaired"] = "started"

    def document(self) -> dict[str, JsonValue]:
        """The launch XCom, without the snapshot that `execute_complete` adds."""
        return {
            "v": LAUNCH_VERSION,
            "run_id": self.run_id,
            "job_id": self.job_id,
            "job_run_id": self.job_run_id,
            "run_page_url": self.run_page_url,
            "repair_id": self.repair_id,
            "baseline": dict(sorted(self.baseline.items())),
            "action": self.action,
        }


def load_project(
    project_dir: str,
    target: str,
    vars_: Mapping[str, Any],
    *,
    expect_plan_hash: str,
    env: Mapping[str, str],
    report: Report,
) -> tuple[Plan, Runtime] | None:
    """Compile and bind the project the launch uploads and runs, as `pipeloom databricks run`
    does. PLX103 when its plan is not the one the DAG rendered."""
    from pipeloom.api import Project, ProjectOptions
    from pipeloom.engine.types import StateStoreCapabilities
    from pipeloom.runtime.run import DEFAULT_CANCEL_GRACE

    project = Project(
        ProjectOptions(project_dir=Path(project_dir), target=target, vars=dict(vars_), env=env)
    )
    compiled = project.compile()
    report.extend(compiled.diagnostics)
    plan = compiled.value
    if plan is None:
        return None
    if plan.metadata.plan_hash.lower() != expect_plan_hash.lower():
        report.add(
            "PLX103",
            f"the project compiles to plan {plan.metadata.plan_hash[:12]}, but the DAG was "
            f"rendered from {expect_plan_hash[:12]}",
            hint="deploy the same project to the scheduler and the workers",
        )
        return None
    bound_outcome = project.bind()
    bound = bound_outcome.value
    if bound is None:
        report.extend(bound_outcome.diagnostics)
        return None
    store = bound.plugins.get("state_stores")
    caps = None if store is None else store.spec.capabilities
    grace = bound.runtime.cancel_grace
    runtime = Runtime(
        artifacts_root=bound.runtime.artifacts.root,
        state_shared=isinstance(caps, StateStoreCapabilities) and caps.shared,
        cancel_grace=int(DEFAULT_CANCEL_GRACE if grace is None else grace),
    )
    return plan, runtime


def start_or_attach(
    client: DatabricksClient,
    project_dir: Path,
    plan: Plan,
    pipeline: str,
    runtime: Runtime,
    *,
    run_id: str,
    full_refresh: bool,
    selection: Selection,
    ensure_job: bool,
    report: Report,
    versions: Mapping[str, str] | None = None,
) -> LaunchResult | None:
    """The job run named `run_id`: started when there is none, repaired when it ended with a
    failed, cancelled or skipped task, else attached as it is. None after reporting why.

    A new run goes through `run_job`: PLD007 without a job, PLD008 for a job deployed from other
    settings, unless `ensure_job` deploys it. A run that exists is continued as it was started.
    """
    meta = plan.metadata
    with api_errors(report):
        job = find_job(
            client, f"{meta.project}.{pipeline}.{meta.target}", name=job_name(plan, pipeline)
        )
        found = None if job is None else find_run(client, job.job_id, run_id)
        if found is None:
            try:
                started = run_job(
                    client,
                    project_dir,
                    plan,
                    pipeline,
                    runtime,
                    selection=selection,
                    run_id=run_id,
                    full_refresh=full_refresh,
                    ensure=ensure_job,
                    report=report,
                    versions=versions,
                )
            except SelectionError as err:
                report.add("PLA002", f"the render selection cannot start a run: {err}")
                return None
            if started is None or started.job_id is None:
                return None
            return LaunchResult(
                run_id, started.job_id, started.job_run_id, started.run_page_url, None
            )
        assert job is not None
        rerun = rerun_set(found) if found.life_cycle_state in TERMINAL else []
        if not rerun:
            return LaunchResult(
                run_id, job.job_id, found.run_id, found.run_page_url, None, action="attached"
            )
        baseline: dict[str, int] = {}
        for t in found.tasks:  # retries may be separate entries: the highest attempt wins
            if t.task_key in rerun:
                baseline[t.task_key] = max(baseline.get(t.task_key, 0), t.attempt + 1)
        repair_id = repair(client, found.run_id, report)
        if repair_id is None:
            return None
        return LaunchResult(
            run_id,
            job.job_id,
            found.run_id,
            found.run_page_url,
            repair_id,
            baseline,
            "repaired",
        )
    return None
