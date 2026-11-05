"""DATABRICKS mode in process: the DAG, the launch (deferral and snapshot), the sensors and the
finalize sensor, all on FakeDatabricks. No connection, workspace or SDK client is ever used."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from pipeloom_airflow import _compat, naming
from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, ProjectConfig
from pipeloom_airflow.dag import PipeloomDag
from pipeloom_airflow.databricks import launch as launch_module
from pipeloom_airflow.testing.context import FakeTaskInstance, make_context
from pipeloom_databricks.api import FINALIZE_KEY, TaskRunInfo
from pipeloom_databricks.testing import fake as fake_module
from pipeloom_databricks.testing.fake import FakeDatabricks

START = datetime(2026, 1, 1)
AFTER = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
FACTORY = "pipeloom_databricks.testing.fake:empty"


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeDatabricks:
    client = FakeDatabricks()
    monkeypatch.setattr(fake_module, "empty", lambda: client)
    return client


def make_dag(dbx_project: Any, **kw: Any) -> PipeloomDag:
    project, manifest, _ = dbx_project
    execution = {
        "mode": ExecutionMode.DATABRICKS,
        "databricks_client_factory": FACTORY,
        "databricks_ensure_job": True,
        "databricks_poll_interval": 5.0,
        **kw,
    }
    return PipeloomDag(
        dag_id="d",
        schedule=None,
        start_date=START,
        project_config=ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
        target="prod",
        execution_config=ExecutionConfig(**execution),
    )


def context(ti: FakeTaskInstance, **params: Any) -> dict[str, Any]:
    return make_context(ti, run_after=AFTER, params=params)


def launch(dag: PipeloomDag, ti: FakeTaskInstance, **params: Any) -> Any:
    op = dag.get_task(naming.LAUNCH_ID)
    with pytest.raises(BaseException) as info:  # TaskDeferred is not an Exception
        op.execute(context(ti, **params))
    assert type(info.value).__name__ == "TaskDeferred", info.value
    return info.value


def finish(fake: FakeDatabricks, job_run_id: int, *results: tuple[str, str]) -> None:
    tasks = [
        TaskRunInfo(k, i, 0, "TERMINATED", r, None, f"{k} {r}") for i, (k, r) in enumerate(results)
    ]
    run = fake.runs[job_run_id]
    state = "SUCCESS" if all(r == "SUCCESS" for _, r in results) else "FAILED"
    fake.set_run(
        job_run_id,
        job_id=run.job_id,
        result_state=state,
        tasks=tasks,
        job_parameters=run.job_parameters,
        run_page_url="https://fake/run",
    )


def complete(dag: PipeloomDag, ti: FakeTaskInstance, deferred: Any) -> dict[str, Any]:
    async def first() -> Any:
        async for event in deferred.trigger.run():
            return event.payload
        raise AssertionError("no event")

    event = asyncio.run(first())
    op = dag.get_task(naming.LAUNCH_ID)
    doc: dict[str, Any] = getattr(op, deferred.method_name)(context(ti), event, **deferred.kwargs)
    ti.xcoms[(naming.LAUNCH_ID, "return_value")] = doc
    return doc


def sense(dag: PipeloomDag, ti: FakeTaskInstance, task_id: str) -> str:
    try:
        dag.get_task(task_id).execute(context(ti))
    except _compat.AirflowSkipException:
        return "skipped"
    except _compat.AirflowFailException:
        return "failed"
    return "success"


def test_the_graph(dbx_project: Any) -> None:
    dag = make_dag(dbx_project)
    launch_op = dag.get_task(naming.LAUNCH_ID)
    assert launch_op.retries == 2
    assert dag.get_task("a").upstream_task_ids == {naming.LAUNCH_ID}
    assert dag.get_task("b").upstream_task_ids == {"a"}
    finalize = dag.get_task(naming.FINALIZE_ID)
    assert finalize.trigger_rule == "all_done" and finalize.task_key == FINALIZE_KEY
    assert finalize.upstream_task_ids == {"b", "c", naming.LAUNCH_ID}
    assert {dag.get_task(k).retries for k in "abc"} == {0}


def test_launch_starts_defers_and_the_sensors_mirror_the_run(
    dbx_project: Any, fake: FakeDatabricks
) -> None:
    dag = make_dag(dbx_project)
    ti = FakeTaskInstance(dag_id="d", run_id="manual__1", task_id=naming.LAUNCH_ID)
    deferred = launch(dag, ti, full_refresh=True)
    rid = naming.airflow_run_id("d", "manual__1")
    assert deferred.method_name == "execute_complete"
    doc = deferred.kwargs["launch"]
    assert (doc["v"], doc["run_id"], doc["action"]) == (1, rid, "started")
    ((_, params, only, _),) = fake.starts
    assert params == {"full_refresh": "true", "pipeloom_run_id": rid} and only is None
    _, kwargs = deferred.trigger.serialize()
    assert kwargs["task_key"] is None and kwargs["poll_interval"] == 5.0
    assert kwargs["job_run_id"] == doc["job_run_id"]

    finish(
        fake,
        doc["job_run_id"],
        ("a", "SUCCESS"),
        ("b", "FAILED"),
        ("c", "EXCLUDED"),
        (FINALIZE_KEY, "FAILED"),
    )
    stored = complete(dag, ti, deferred)
    assert stored["snapshot"]["result_state"] == "FAILED"
    outcomes = {t: sense(dag, ti, t) for t in ("a", "b", "c", naming.FINALIZE_ID)}
    assert outcomes == {"a": "success", "b": "failed", "c": "skipped", naming.FINALIZE_ID: "failed"}


def test_a_cleared_launch_repairs_the_failed_run(dbx_project: Any, fake: FakeDatabricks) -> None:
    dag = make_dag(dbx_project)
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id=naming.LAUNCH_ID)
    first = launch(dag, ti)
    job_run_id = first.kwargs["launch"]["job_run_id"]
    finish(fake, job_run_id, ("a", "SUCCESS"), ("b", "FAILED"), (FINALIZE_KEY, "FAILED"))
    again = launch(dag, ti)
    doc = again.kwargs["launch"]
    assert doc["action"] == "repaired" and doc["job_run_id"] == job_run_id
    assert doc["baseline"] == {"b": 1, FINALIZE_KEY: 1}
    assert again.trigger.serialize()[1]["repair_id"] == doc["repair_id"]
    assert len(fake.starts) == 1 and len(fake.repairs) == 1


def test_launch_errors(dbx_project: Any, fake: FakeDatabricks) -> None:
    dag = make_dag(dbx_project, databricks_ensure_job=False)
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id=naming.LAUNCH_ID)
    with pytest.raises(_compat.AirflowFailException, match="PLD007"):
        dag.get_task(naming.LAUNCH_ID).execute(context(ti))
    fake.fail["list_jobs"] = fake_module.ApiError(503, "unavailable")
    with pytest.raises(_compat.AirflowException, match="PLD009") as info:
        dag.get_task(naming.LAUNCH_ID).execute(context(ti))
    assert not isinstance(info.value, _compat.AirflowFailException)  # a retry may clear it
    op = dag.get_task(naming.LAUNCH_ID)
    with pytest.raises(_compat.AirflowException, match="denied"):
        op.execute_complete(context(ti), {"status": "error", "message": "denied"}, launch={})


def test_the_airflow_connection_maps_to_explicit_credentials(
    dbx_project: Any, fake: FakeDatabricks, monkeypatch: pytest.MonkeyPatch
) -> None:
    class Conn:
        host = "adb-1.example.net"
        login = None
        password = "dapi-secret"
        extra_dejson: dict[str, Any] = {}

    seen: list[dict[str, Any]] = []
    masked: list[str] = []
    monkeypatch.setattr(launch_module, "get_connection", lambda conn_id: Conn())
    monkeypatch.setattr(launch_module, "mask_secret", masked.append)
    from pipeloom_databricks import api

    monkeypatch.setattr(api, "connect_explicit", lambda **kw: seen.append(kw) or fake)
    dag = make_dag(dbx_project, databricks_client_factory=None)
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id=naming.LAUNCH_ID)
    launch(dag, ti)
    assert seen[0]["host"] == "https://adb-1.example.net" and seen[0]["auth_type"] == "pat"
    assert masked == ["dapi-secret"]


def test_a_sensor_without_a_snapshot_fails(dbx_project: Any) -> None:
    dag = make_dag(dbx_project)
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id="a")
    assert sense(dag, ti, "a") == "failed"
    assert sense(dag, ti, naming.FINALIZE_ID) == "failed"
