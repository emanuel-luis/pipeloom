"""A local stand-in for the Jobs service: run a rendered job through the task wrapper.

It substitutes the dynamic value references, runs the tasks in dependency order through
`pipeloom_databricks.task.main` in this process, and honors `run_if`, `only` and a repair.
It starts no cluster and calls no API. The project's engine, state store and artifacts root
come from its own configuration, so a test points them at a temporary directory.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from pydantic import JsonValue

from pipeloom_databricks.client import (
    ConnectionNotConfigured,
    DatabricksClient,
    TaskRunInfo,
)
from pipeloom_databricks.task import main
from pipeloom_databricks.task_values import TASK_VALUE_KEY
from pipeloom_databricks.testing.fake import FakeDatabricks

START_MS = 1_700_000_000_000
"""The job start every simulated run (and repair) reports."""
_REFERENCE = re.compile(r"\{\{([^{}]+)\}\}")


@dataclass
class SimRun:
    job_run_id: int
    task_states: dict[str, str] = field(default_factory=dict)
    """SUCCESS, FAILED, UPSTREAM_FAILED or EXCLUDED, for the tasks this call ran or skipped."""
    exit_codes: dict[str, int] = field(default_factory=dict)
    """The exit code of every task that ran."""
    task_values: dict[str, Any] = field(default_factory=dict)
    """The `pipeloom` task value each task set, by task key."""


class _Recorder:
    def __init__(self, run: SimRun, task_key: str) -> None:
        self._run = run
        self._key = task_key

    def set(self, key: str, value: JsonValue) -> None:
        if key == TASK_VALUE_KEY:
            self._run.task_values[self._key] = value


def _order(tasks: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """Dependencies first, ties by key."""
    done: list[str] = []
    waiting = sorted(tasks)
    while waiting:
        ready = [
            k for k in waiting if all(d["task_key"] in done for d in tasks[k].get("depends_on", []))
        ]
        if not ready:
            raise ValueError("the job's tasks form a cycle or depend on an unknown task")
        done.append(ready[0])
        waiting.remove(ready[0])
    return done


def _no_client() -> DatabricksClient:
    raise ConnectionNotConfigured("the simulator was given no client")


def _record(client: FakeDatabricks, run: SimRun, job_id: int, params: Mapping[str, str]) -> None:
    """Put the task states known so far into the fake's run, as the Jobs API would report them
    to the finalize task. A repair keeps the tasks it does not run again."""
    old = client.runs.get(run.job_run_id)
    tasks = {t.task_key: t for t in old.tasks} if old else {}
    for key, state in run.task_states.items():
        failed = state == "FAILED"
        tasks[key] = TaskRunInfo(
            key,
            run.job_run_id * 1000 + len(tasks),
            1,
            "TERMINATED",
            state,
            "RUN_EXECUTION_ERROR" if failed else None,
            "the task failed" if failed else None,
        )
    client.set_run(
        run.job_run_id,
        job_id=job_id,
        life_cycle_state="RUNNING",
        tasks=[tasks[k] for k in sorted(tasks)],
        job_parameters=params,
        latest_repair_id=old.latest_repair_id if old else None,
    )


def simulate_job(
    settings: Mapping[str, JsonValue],
    *,
    job_run_id: int,
    job_parameters: Mapping[str, str],
    fail: Collection[str] = (),
    only: Collection[str] | None = None,
    rerun: Collection[str] | None = None,
    job_id: int = 1,
    start_ms: int = START_MS,
    client: FakeDatabricks | None = None,
) -> SimRun:
    """Run the job once.

    `fail` names tasks that die before they run anything (a lost cluster): exit 1, no attempt
    file. `only` runs just those tasks; the others are EXCLUDED. `rerun` is a repair: only
    those tasks run again, the others keep what an earlier call did. With `client`, the task
    states of the run are kept in the fake as they happen, and finalize reads them from it; without
    one finalize finds no connection and runs without states.
    """
    job: dict[str, Any] = dict(settings)
    tasks: dict[str, Mapping[str, Any]] = {t["task_key"]: t for t in job["tasks"]}
    params: dict[str, str] = {}
    for item in job.get("parameters") or []:
        params[item["name"]] = item["default"]
    params.update(job_parameters)

    run = SimRun(job_run_id)
    factory = _no_client if client is None else (lambda: client)
    for position, key in enumerate(_order(tasks), start=1):
        task = tasks[key]
        if only is not None and key not in only:
            run.task_states[key] = "EXCLUDED"
            continue
        if rerun is not None and key not in rerun:
            continue
        if client is not None:
            _record(client, run, job_id, params)
        seen = [
            run.task_states.get(d["task_key"], "SUCCESS")
            for d in task.get("depends_on", [])
            if run.task_states.get(d["task_key"]) != "EXCLUDED"
        ]
        run_if = task.get("run_if") or "ALL_SUCCESS"
        if run_if not in ("ALL_SUCCESS", "ALL_DONE"):
            raise ValueError(f"run_if {run_if} is not simulated")
        if run_if == "ALL_SUCCESS" and any(s != "SUCCESS" for s in seen):
            run.task_states[key] = "UPSTREAM_FAILED"
            continue
        if key in fail:
            run.task_states[key], run.exit_codes[key] = "FAILED", 1
            continue
        values = {
            "job.id": str(job_id),
            "job.run_id": str(job_run_id),
            "job.start_time.timestamp_ms": str(start_ms),
            "task.name": key,
            "task.run_id": str(job_run_id * 1000 + position),
        }

        def substitute(match: re.Match[str], values: dict[str, str] = values) -> str:
            name = match.group(1).strip()
            if name.startswith("job.parameters."):
                return params.get(name.removeprefix("job.parameters."), match.group(0))
            return values.get(name, match.group(0))

        argv = [_REFERENCE.sub(substitute, p) for p in task["python_wheel_task"]["parameters"]]
        try:
            main(argv, task_values=_Recorder(run, key), client_factory=factory)
            code = 0
        except SystemExit as stop:
            code = stop.code if isinstance(stop.code, int) else 0
        run.exit_codes[key] = code
        run.task_states[key] = "SUCCESS" if code == 0 else "FAILED"
    if client is not None:
        _record(client, run, job_id, params)
        done = client.runs[job_run_id]
        bad = any(t.result_state not in ("SUCCESS", "EXCLUDED") for t in done.tasks)
        client.runs[job_run_id] = replace(
            done, life_cycle_state="TERMINATED", result_state="FAILED" if bad else "SUCCESS"
        )
    return run
