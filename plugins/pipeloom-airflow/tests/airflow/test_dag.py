"""PipeloomDag and PipeloomTaskGroup built in process: the graph, the params, the operator
arguments and the errors a broken configuration raises at parse time."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeloom_airflow import _compat, naming
from pipeloom_airflow import dag as dag_module
from pipeloom_airflow.config import (
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProjectConfig,
    RenderConfig,
)
from pipeloom_airflow.dag import PipeloomDag, PipeloomTaskGroup
from pipeloom_airflow.errors import PipeloomAirflowError

START = datetime(2026, 1, 1)


def project_config(af_project: Any) -> ProjectConfig:
    project, manifest, _ = af_project
    return ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix())


def make_dag(af_project: Any, **kw: Any) -> PipeloomDag:
    return PipeloomDag(
        dag_id="d",
        schedule=None,
        start_date=START,
        project_config=project_config(af_project),
        target="dev",
        **kw,
    )


def test_the_dag_is_a_dag_with_one_task_per_unit(af_project: Any, af_plan: Any) -> None:
    dag = make_dag(af_project)
    assert isinstance(dag, _compat.DAG)
    plan = af_plan[0]
    assert set(dag.task_ids) == {t.task_id for t in plan.tasks} | {naming.FINALIZE_ID}
    finalize = dag.get_task(naming.FINALIZE_ID)
    assert finalize.upstream_task_ids == set(plan.leaves)
    assert dag.params["full_refresh"] is False


def test_a_full_refresh_param_of_the_caller_is_kept(af_project: Any) -> None:
    dag = make_dag(af_project, params={"full_refresh": True, "other": 1})
    assert dag.params["full_refresh"] is True and dag.params["other"] == 1


def test_operator_args_and_retries_reach_the_operators(af_project: Any, af_plan: Any) -> None:
    dag = make_dag(af_project, operator_args={"pool": "pipeloom", "queue": "q"})
    for t in af_plan[0].tasks:
        op = dag.get_task(t.task_id)
        assert op.pool == "pipeloom" and op.queue == "q"
        assert op.retries == t.retries
        assert op.retry_delay == timedelta(seconds=t.retry_delay_s)
    assert dag.get_task(naming.FINALIZE_ID).pool == "pipeloom"


def test_a_task_group_prefixes_its_tasks_and_its_run_id(af_project: Any, af_plan: Any) -> None:
    with _compat.DAG(dag_id="outer", schedule=None, start_date=START) as dag:
        PipeloomTaskGroup(
            "shop",
            project_config=project_config(af_project),
            target="dev",
            render_config=RenderConfig(group_by="tag"),
        )
    ids = set(dag.task_ids)
    assert f"shop.{naming.FINALIZE_ID}" in ids
    assert {f"shop.untagged.{t.task_id}" for t in af_plan[0].tasks} <= ids
    finalize = dag.get_task(f"shop.{naming.FINALIZE_ID}")
    assert finalize.group_id == "shop"
    assert set(finalize.task_units) == {f"shop.untagged.{t.task_id}" for t in af_plan[0].tasks}


@pytest.mark.parametrize(
    ("kw", "code"),
    [
        ({"execution_config": ExecutionConfig(mode=ExecutionMode.DOCKER)}, "PLA001"),
        ({"execution_config": ExecutionConfig(mode=ExecutionMode.DATABRICKS)}, "PLA001"),
        ({"execution_config": ExecutionConfig(mode=ExecutionMode.KUBERNETES)}, "PLA002"),
        ({"operator_args": {"retries": 3}}, "PLA002"),
    ],
)
def test_a_broken_configuration_raises_at_parse_time(
    af_project: Any, kw: dict[str, Any], code: str
) -> None:
    with pytest.raises(PipeloomAirflowError) as info:
        make_dag(af_project, **kw)
    assert code in {d.code for d in info.value.diagnostics}


def test_an_old_airflow_is_pla005(af_project: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dag_module, "airflow_version", lambda: "3.1.7")
    with pytest.raises(PipeloomAirflowError) as info:
        make_dag(af_project)
    assert [d.code for d in info.value.diagnostics] == ["PLA005"]


def test_warnings_go_to_the_logger(af_project: Any, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING, logger="pipeloom_airflow"):
        dag = make_dag(af_project, render_config=RenderConfig(select=("tag:nothing",)))
    assert any("PLA003" in r.getMessage() for r in caplog.records)
    assert dag.task_ids == [naming.FINALIZE_ID]


def test_a_project_load_compiles_once_into_the_cache(af_project: Any, tmp_path: Path) -> None:
    project, _, _ = af_project
    config = ProjectConfig(project.as_posix(), cache_dir=(tmp_path / "cache").as_posix())
    for _ in range(2):
        dag = PipeloomDag(
            dag_id="d",
            schedule=None,
            start_date=START,
            project_config=config,
            target="dev",
            render_config=RenderConfig(load_method=LoadMode.PROJECT),
        )
    assert len(list((tmp_path / "cache").rglob("*.json"))) >= 1
    assert naming.FINALIZE_ID in dag.task_ids
