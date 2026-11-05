"""PipeloomRunOperator with real `pipeloom run` children and a fake task context."""

from __future__ import annotations

import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeloom.artifacts.layout import RunPaths
from pipeloom_airflow import naming
from pipeloom_airflow._compat import AirflowException, AirflowFailException
from pipeloom_airflow.config import ExecutionConfig, ProjectConfig, RenderConfig
from pipeloom_airflow.dag import _plan
from pipeloom_airflow.operators import PipeloomRunOperator
from pipeloom_airflow.render import TaskPlan
from pipeloom_airflow.testing.context import FakeTaskInstance, make_context

AFTER = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
Project = tuple[Path, Path, str]


def plan_for(af_project: Project, execution: ExecutionConfig | None = None) -> tuple[TaskPlan, Any]:
    project, manifest, _ = af_project
    return _plan(
        ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
        "dev",
        RenderConfig(),
        execution or ExecutionConfig(),
        {},
    )


def operator(
    af_project: Project, model: str, *, env: dict[str, str] | None = None, **kw: Any
) -> PipeloomRunOperator:
    plan, command = plan_for(af_project)
    task = next(t for t in plan.tasks if t.models == (model,))
    return PipeloomRunOperator(
        task_id=task.task_id,
        unit_id=task.unit_id,
        command=command,
        venv_dir=kw.pop("venv_dir", None),
        env=env or {},
        retry_on=kw.pop("retry_on", task.retry_on),
        cancel_grace_s=plan.cancel_grace_s,
        group_id=None,
        **kw,
    )


def run(op: PipeloomRunOperator, *, params: dict[str, Any] | None = None) -> None:
    ti = FakeTaskInstance(dag_id="d", run_id="manual__1", task_id=op.task_id)
    op.execute(make_context(ti, run_after=AFTER, params=params or {}))


def attempts(af_project: Project, unit_id: str) -> list[int]:
    rid = naming.airflow_run_id("d", "manual__1")
    return RunPaths(af_project[0] / "target", rid).attempt_numbers(unit_id)


def test_a_unit_runs_and_a_rerun_with_the_same_run_id_is_attempt_2(af_project: Project) -> None:
    op = operator(af_project, "base")
    run(op)
    assert attempts(af_project, op.unit_id) == [1]
    run(op)
    assert attempts(af_project, op.unit_id) == [1, 2]


def test_a_permanent_failure_does_not_retry(af_project: Project) -> None:
    op = operator(af_project, "other", env={"AFPROJ_MODE": "permanent"})
    with pytest.raises(AirflowFailException, match="bad data"):
        run(op)


def test_a_permanent_failure_retries_with_retry_on_any(af_project: Project) -> None:
    op = operator(af_project, "other", env={"AFPROJ_MODE": "permanent"}, retry_on="any")
    with pytest.raises(AirflowException) as info:
        run(op)
    assert not isinstance(info.value, AirflowFailException)


def test_a_transient_failure_retries(af_project: Project) -> None:
    op = operator(af_project, "other", env={"AFPROJ_MODE": "transient"})
    with pytest.raises(AirflowException, match="network blip") as info:
        run(op)
    assert not isinstance(info.value, AirflowFailException)


def test_on_kill_stops_a_running_unit(af_project: Project) -> None:
    op = operator(af_project, "other", env={"AFPROJ_MODE": "sleep"})
    errors: list[BaseException] = []

    def target() -> None:
        try:
            run(op)
        except BaseException as err:  # noqa: BLE001 - collected for the assertion
            errors.append(err)

    worker = threading.Thread(target=target)
    worker.start()
    deadline = time.monotonic() + 30
    while op._proc is None and time.monotonic() < deadline:
        time.sleep(0.1)
    time.sleep(3)  # let the child start the model
    start = time.monotonic()
    op.on_kill()
    worker.join(30)
    assert not worker.is_alive()
    assert time.monotonic() - start < op.cancel_grace_s + 10
    assert op._proc is not None and op._proc.poll() is not None
    assert errors and isinstance(errors[0], AirflowException)


def test_a_missing_virtualenv_fails_without_retry(af_project: Project, tmp_path: Path) -> None:
    op = operator(af_project, "base", venv_dir=(tmp_path / "nope").as_posix())
    with pytest.raises(AirflowFailException, match="PLA002"):
        run(op)


def test_a_virtualenv_interpreter_runs_the_unit(af_project: Project) -> None:
    # the test environment is itself a virtualenv with pipeloom installed
    assert sys.prefix != sys.base_prefix
    op = operator(af_project, "base", venv_dir=sys.prefix)
    run(op)
    assert attempts(af_project, op.unit_id) == [1]


def test_a_bad_full_refresh_param_fails_without_retry(af_project: Project) -> None:
    op = operator(af_project, "base")
    with pytest.raises(AirflowFailException, match="PLA002"):
        run(op, params={"full_refresh": "maybe"})
