"""The command lines a task runs, and the run identity every task of a DAG run shares."""

from __future__ import annotations

import ntpath
import posixpath
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from pipeloom.diagnostics import Report
from pipeloom.platform import check_orchestrator_id
from pipeloom_airflow import naming
from pipeloom_airflow.errors import PipeloomAirflowError


@dataclass(frozen=True)
class RunContext:
    run_id: str
    run_started_at: datetime
    full_refresh: bool
    orchestrator_ids: Mapping[str, str]


def _bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    return None


def run_context(
    *,
    dag_id: str,
    airflow_run_id: str,
    run_after: datetime,
    task_id: str,
    try_number: int,
    group_id: str | None,
    full_refresh: object,
) -> RunContext:
    """What a task knows about its run. `full_refresh` is a bool or "true"/"false"; anything
    else is PLA002. A naive `run_after` is read as UTC."""
    refresh = _bool(full_refresh)
    if refresh is None:
        report = Report()
        report.add(
            "PLA002",
            f"full_refresh must be true or false, got {full_refresh!r}",
            hint="trigger the DAG with the param full_refresh set to a boolean",
        )
        raise PipeloomAirflowError(report.sorted())
    started = run_after.replace(tzinfo=UTC) if run_after.tzinfo is None else run_after
    ids = {
        "airflow_dag_id": dag_id,
        "airflow_run_id": airflow_run_id,
        "airflow_task_id": task_id,
        "airflow_try_number": str(try_number),
    }
    for key, value in ids.items():
        check_orchestrator_id(key, value)
    return RunContext(
        run_id=naming.airflow_run_id(dag_id, airflow_run_id, group_id),
        run_started_at=started.astimezone(UTC),
        full_refresh=refresh,
        orchestrator_ids=ids,
    )


def _ids(ctx: RunContext) -> list[str]:
    return [a for k, v in ctx.orchestrator_ids.items() for a in ("--orchestrator-id", f"{k}={v}")]


@dataclass(frozen=True)
class CoreCommand:
    """The core invocation a task makes. PROJECT mode passes no manifest: every unit compiles
    and checks the plan hash, so no cache path crosses a process boundary."""

    project_dir: str
    target: str
    manifest: str | None
    plan_hash: str
    vars_json: str | None

    def _base(self) -> list[str]:
        return ["--project-dir", self.project_dir, "--target", self.target]

    def _plan(self) -> list[str]:
        return (["--manifest", self.manifest] if self.manifest else []) + [
            "--expect-plan-hash",
            self.plan_hash,
        ]

    def _vars(self) -> list[str]:
        return ["--vars", self.vars_json] if self.vars_json else []

    def run_argv(
        self,
        python: Sequence[str],
        unit_id: str,
        ctx: RunContext,
        *,
        log_format: Literal["json", "text"],
    ) -> list[str]:
        return [
            *python,
            "-m",
            "pipeloom",
            "run",
            "--log-format",
            log_format,
            *self._base(),
            *self._plan(),
            "--select",
            f"unit:{unit_id}",
            "--run-id",
            ctx.run_id,
            "--run-started-at",
            ctx.run_started_at.isoformat(),
            "--full-refresh",
            "true" if ctx.full_refresh else "false",
            *_ids(ctx),
            *self._vars(),
        ]

    def finalize_argv(
        self,
        python: Sequence[str],
        ctx: RunContext,
        *,
        select: Sequence[str],
        exclude: Sequence[str],
        unit_states_path: str | None = None,
        unit_states_json: str | None = None,
    ) -> list[str]:
        states = ["--unit-states", unit_states_path] if unit_states_path is not None else []
        if unit_states_json is not None:
            states += ["--unit-states-json", unit_states_json]
        return [
            *python,
            "-m",
            "pipeloom",
            "finalize",
            *self._base(),
            *self._plan(),
            "--run-id",
            ctx.run_id,
            "--run-started-at",
            ctx.run_started_at.isoformat(),
            *[a for s in select for a in ("--select", s)],
            *[a for s in exclude for a in ("--exclude", s)],
            *_ids(ctx),
            *states,
            *self._vars(),
        ]


def venv_python(venv_dir: str, *, platform: str) -> str:
    """The interpreter of a virtualenv: `bin/python`, or `Scripts/python.exe` on win32."""
    if platform == "win32":
        return ntpath.join(venv_dir, "Scripts", "python.exe")
    return posixpath.join(venv_dir, "bin", "python")
