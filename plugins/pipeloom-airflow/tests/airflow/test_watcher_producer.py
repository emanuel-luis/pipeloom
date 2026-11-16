"""PipeloomWatcherProducerOperator with a real `pipeloom run` child and a fake task context."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeloom.artifacts.layout import RunPaths
from pipeloom_airflow import naming
from pipeloom_airflow._compat import AirflowFailException, AirflowSkipException
from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, ProjectConfig
from pipeloom_airflow.dag import PipeloomDag
from pipeloom_airflow.testing.context import FakeTaskInstance, make_context

AFTER = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
RUN_ID = "manual__1"
Project = tuple[Path, Path, str]


def producer(af_project: Project, **execution: Any) -> Any:
    project, manifest, _ = af_project
    dag = PipeloomDag(
        dag_id="d",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        project_config=ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
        target="dev",
        execution_config=ExecutionConfig(mode=ExecutionMode.WATCHER, **execution),
    )
    return dag.get_task(naming.PRODUCER_ID)


def run(op: Any, try_number: int = 1) -> None:
    ti = FakeTaskInstance(dag_id="d", run_id=RUN_ID, task_id=op.task_id, try_number=try_number)
    op.execute(make_context(ti, run_after=AFTER, params={}))


def paths(af_project: Project) -> RunPaths:
    return RunPaths(af_project[0] / "target", naming.airflow_run_id("d", RUN_ID))


def units(af_project: Project) -> list[str]:
    return sorted(p.name for p in paths(af_project).units_dir.iterdir())


def test_the_producer_runs_every_unit_once(af_project: Project, af_plan: Any) -> None:
    op = producer(af_project, watcher_workers=2)
    run(op)
    assert paths(af_project).run_results.is_file()
    assert units(af_project) == sorted(t.unit_id for t in af_plan[0].tasks)
    for unit_id in units(af_project):
        assert paths(af_project).attempt_numbers(unit_id) == [1]


def test_a_failed_pipeloom_run_is_a_producer_success(af_project: Project, af_plan: Any) -> None:
    op = producer(af_project, env={"AFPROJ_MODE": "permanent"})
    run(op)  # exit 1: the watcher of `other` reports the failure
    other = next(t.unit_id for t in af_plan[0].tasks if t.models == ("other",))
    assert paths(af_project).attempt_numbers(other) == [1]


def test_a_second_try_is_skipped(af_project: Project) -> None:
    op = producer(af_project)
    with pytest.raises(AirflowSkipException):
        run(op, try_number=2)
    assert not paths(af_project).run_dir.exists()


def test_results_missing_under_the_root_is_pla008(af_project: Project, tmp_path: Path) -> None:
    op = producer(af_project, watcher_artifacts_root=(tmp_path / "elsewhere").as_posix())
    op.visibility_grace_s = 0.0
    with pytest.raises(AirflowFailException, match="PLA008"):
        run(op)
    assert paths(af_project).run_results.is_file()  # the run wrote under its own root


def test_a_usage_error_fails_the_producer(af_project: Project) -> None:
    op = producer(af_project)
    op.command = replace(op.command, plan_hash="abc")
    with pytest.raises(AirflowFailException, match="exited with 2"):
        run(op)
