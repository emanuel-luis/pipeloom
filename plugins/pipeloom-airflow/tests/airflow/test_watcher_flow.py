"""A WATCHER DAG run end to end with real children and fake task contexts: the producer, every
watcher, finalize; then the failed watcher cleared with Downstream reruns its unit."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeloom.artifacts.layout import RunPaths
from pipeloom_airflow import naming
from pipeloom_airflow._compat import AirflowFailException
from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, ProjectConfig
from pipeloom_airflow.dag import PipeloomDag
from pipeloom_airflow.testing.context import FakeTaskInstance, make_context

AFTER = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
RUN_ID = "manual__1"
Project = tuple[Path, Path, str]


def make_dag(af_project: Project, env: dict[str, str]) -> PipeloomDag:
    project, manifest, _ = af_project
    return PipeloomDag(
        dag_id="d",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        project_config=ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
        target="dev",
        execution_config=ExecutionConfig(mode=ExecutionMode.WATCHER, env=env),
    )


def execute(op: Any, states: dict[str, str | None], try_number: int = 1) -> None:
    ti = FakeTaskInstance(
        dag_id="d", run_id=RUN_ID, task_id=op.task_id, try_number=try_number, states=states
    )
    op.execute(make_context(ti, run_after=AFTER, params={}))


def test_a_failed_unit_fails_its_watcher_and_a_clear_reruns_only_it(
    af_project: Project, af_plan: Any
) -> None:
    by_model = {t.models[0]: t for t in af_plan[0].tasks}
    base, child, other = by_model["base"], by_model["child"], by_model["other"]
    paths = RunPaths(af_project[0] / "target", naming.airflow_run_id("d", RUN_ID))

    broken = make_dag(af_project, {"AFPROJ_BASE": "fail"})
    execute(broken.get_task(naming.PRODUCER_ID), {})  # exit 1 is still a producer success
    rows = json.loads(paths.run_results.read_text(encoding="utf-8"))["results"]
    status = {r["unit_id"]: r["status"] for r in rows}
    assert (status[base.unit_id], status[child.unit_id]) == ("failed", "skipped")

    produced = {naming.PRODUCER_ID: "success"}
    with pytest.raises(AirflowFailException, match="bad base"):
        execute(broken.get_task(base.task_id), produced)
    execute(broken.get_task(other.task_id), produced)
    # Airflow does not start `child`: its upstream failed
    states = {
        **produced,
        base.task_id: "failed",
        child.task_id: "upstream_failed",
        other.task_id: "success",
    }
    with pytest.raises(AirflowFailException, match="failed"):
        execute(broken.get_task(naming.FINALIZE_ID), states)

    # the input is fixed and `base` is cleared with Downstream: base at try 2, child at try 1
    fixed = make_dag(af_project, {})
    execute(fixed.get_task(base.task_id), produced, try_number=2)
    execute(fixed.get_task(child.task_id), produced)
    states = {**states, base.task_id: "success", child.task_id: "success"}
    execute(fixed.get_task(naming.FINALIZE_ID), states)

    assert paths.attempt_numbers(base.unit_id) == [1, 2]
    assert paths.attempt_numbers(child.unit_id) == [1]
    assert paths.attempt_numbers(other.unit_id) == [1]
    rerun = json.loads(paths.attempt(base.unit_id, 2).read_text(encoding="utf-8"))
    assert rerun["status"] == "succeeded"
    assert rerun["orchestrator_ids"]["airflow_task_id"] == base.task_id
    rows = json.loads(paths.run_results.read_text(encoding="utf-8"))["results"]
    assert {r["status"] for r in rows} == {"succeeded"}
