"""DATABRICKS mode in process: the DAG, the launch (deferral and snapshot), the sensors and the
finalize sensor, all on FakeDatabricks. No connection, workspace or SDK client is ever used."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

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
        "databricks_poll": "shared",
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


def test_the_wait_is_bounded_and_a_timed_out_launch_attaches_on_retry(
    dbx_project: Any, fake: FakeDatabricks
) -> None:
    assert launch(make_dag(dbx_project), FakeTaskInstance("d", "r", naming.LAUNCH_ID)).timeout == (
        timedelta(hours=24)
    )
    dag = make_dag(dbx_project, databricks_timeout=90.0)
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id=naming.LAUNCH_ID)
    first = launch(dag, ti)
    assert first.timeout == timedelta(seconds=90)
    # the deferral timed out while the run kept going; Airflow retries the launch
    retry = FakeTaskInstance(dag_id="d", run_id="r", task_id=naming.LAUNCH_ID, try_number=2)
    again = launch(dag, retry)
    doc = again.kwargs["launch"]
    assert doc["action"] == "attached"
    assert doc["job_run_id"] == first.kwargs["launch"]["job_run_id"]
    assert len(fake.starts) == 1 and fake.repairs == []


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
        extra_dejson: ClassVar[dict[str, Any]] = {}

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


def per_task_launch(dag: PipeloomDag, ti: FakeTaskInstance, **params: Any) -> dict[str, Any]:
    doc: dict[str, Any] = dag.get_task(naming.LAUNCH_ID).execute(context(ti, **params))
    ti.xcoms[(naming.LAUNCH_ID, "return_value")] = doc
    return doc


def test_per_task_is_the_default_and_the_launch_returns_at_once(
    dbx_project: Any, fake: FakeDatabricks
) -> None:
    dag = make_dag(dbx_project, databricks_poll="per_task")
    assert ExecutionConfig().databricks_poll == "per_task"
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id=naming.LAUNCH_ID)
    doc = per_task_launch(dag, ti)
    assert doc["action"] == "started" and "snapshot" not in doc
    # the launch only reads the run it just started, once; it never waits for it
    assert len(fake.starts) == 1 and fake.called("get_run") == [str(doc["job_run_id"])]
    sensor = dag.get_task("a")
    assert (sensor.poll, sensor.conn_id, sensor.poll_interval) == (
        "per_task",
        "databricks_default",
        5.0,
    )
    assert sensor.timeout == 24 * 3600
    custom = make_dag(dbx_project, databricks_poll="per_task", databricks_timeout=90.0)
    assert (
        custom.get_task("a").timeout == 90.0 and custom.get_task(naming.FINALIZE_ID).timeout == 90.0
    )


def test_a_sensor_with_a_verdict_makes_one_call_and_does_not_defer(
    dbx_project: Any, fake: FakeDatabricks
) -> None:
    dag = make_dag(dbx_project, databricks_poll="per_task")
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id="a")
    doc = per_task_launch(dag, ti)
    finish(
        fake,
        doc["job_run_id"],
        ("a", "SUCCESS"),
        ("b", "FAILED"),
        ("c", "EXCLUDED"),
        (FINALIZE_KEY, "FAILED"),
    )
    before = len(fake.called("get_run"))
    outcomes = {t: sense(dag, ti, t) for t in ("a", "b", "c", naming.FINALIZE_ID)}
    assert outcomes == {"a": "success", "b": "failed", "c": "skipped", naming.FINALIZE_ID: "failed"}
    assert fake.called("get_run")[before:] == [str(doc["job_run_id"])] * 4


def test_a_sensor_without_a_verdict_defers_on_its_own_trigger_and_settles(
    dbx_project: Any, fake: FakeDatabricks, monkeypatch: pytest.MonkeyPatch
) -> None:
    dag = make_dag(dbx_project, databricks_poll="per_task")
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id="a")
    doc = per_task_launch(dag, ti)
    job_run_id = doc["job_run_id"]
    run = fake.runs[job_run_id]
    fake.set_run(job_run_id, job_id=run.job_id, life_cycle_state="RUNNING", tasks=[])
    sensor = dag.get_task("a")
    with pytest.raises(BaseException) as info:  # TaskDeferred is not an Exception
        sensor.execute(context(ti))
    deferred = info.value
    assert type(deferred).__name__ == "TaskDeferred"
    assert deferred.method_name == "execute_complete"
    assert deferred.timeout.total_seconds() == 24 * 3600
    _, kwargs = deferred.trigger.serialize()
    assert (kwargs["task_key"], kwargs["job_run_id"], kwargs["poll_interval"]) == (
        "a",
        job_run_id,
        5.0,
    )

    async def sleep(seconds: float) -> None:
        fake.set_run(
            job_run_id,
            job_id=run.job_id,
            life_cycle_state="RUNNING",
            tasks=[TaskRunInfo("a", 1, 0, "TERMINATED", "SUCCESS", None, "ok")],
        )

    monkeypatch.setattr(type(deferred.trigger), "sleep", staticmethod(sleep))

    async def first() -> Any:
        async for event in deferred.trigger.run():
            return event.payload
        raise AssertionError("no event")

    event = asyncio.run(first())
    assert sensor.execute_complete(context(ti), event) is None
    failed = {**event, "outcome": "failed", "message": "boom"}
    with pytest.raises(_compat.AirflowFailException, match="boom"):
        sensor.execute_complete(context(ti), failed)


def test_a_per_task_sensor_reads_the_baseline_and_reports_api_errors(
    dbx_project: Any, fake: FakeDatabricks
) -> None:
    dag = make_dag(dbx_project, databricks_poll="per_task")
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id="b")
    assert sense(dag, ti, "b") == "failed"  # no launch result
    doc = per_task_launch(dag, ti)
    ti.xcoms[(naming.LAUNCH_ID, "return_value")] = {**doc, "baseline": {"b": 1}}
    job_run_id = doc["job_run_id"]
    fake.set_run(
        job_run_id,
        job_id=doc["job_id"],
        life_cycle_state="RUNNING",
        tasks=[TaskRunInfo("b", 1, 0, "TERMINATED", "FAILED", None, "stale")],
    )
    with pytest.raises(BaseException) as info:
        dag.get_task("b").execute(context(ti))
    assert type(info.value).__name__ == "TaskDeferred"  # attempt 0 is below the baseline
    fake.fail["get_run"] = fake_module.ApiError(403, "denied")
    with pytest.raises(_compat.AirflowException, match="PLD009"):
        dag.get_task("b").execute(context(ti))
    # a throttled or failing API says nothing about the task: the sensor defers instead of failing
    fake.fail["get_run"] = fake_module.ApiError(503, "unavailable")
    with pytest.raises(BaseException) as again:
        dag.get_task("b").execute(context(ti))
    assert type(again.value).__name__ == "TaskDeferred"


def test_after_a_repair_a_stale_poll_does_not_fail_the_sensor(
    dbx_project: Any, fake: FakeDatabricks, monkeypatch: pytest.MonkeyPatch
) -> None:
    dag = make_dag(dbx_project, databricks_poll="per_task")
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id="b")
    first = per_task_launch(dag, ti)
    job_run_id = first["job_run_id"]
    finish(fake, job_run_id, ("a", "SUCCESS"), ("b", "FAILED"), (FINALIZE_KEY, "FAILED"))
    fake.stale_polls = 2  # the next two polls after the repair still show the old, failed run
    doc = per_task_launch(dag, ti)
    assert doc["action"] == "repaired" and doc["baseline"]["b"] == 1
    sensor = dag.get_task("b")
    with pytest.raises(BaseException) as info:
        sensor.execute(context(ti))
    deferred = info.value
    assert type(deferred).__name__ == "TaskDeferred"
    _, kwargs = deferred.trigger.serialize()
    assert kwargs["repair_id"] == doc["repair_id"] and kwargs["baseline_attempt"] == 1

    run = fake.runs[job_run_id]
    polls = {"n": 0}

    async def sleep(seconds: float) -> None:
        polls["n"] += 1
        if polls["n"] == 2:  # the repair finishes
            fake.set_run(
                job_run_id,
                job_id=run.job_id,
                result_state="SUCCESS",
                tasks=[TaskRunInfo("b", 1, 1, "TERMINATED", "SUCCESS", None, "ok")],
                latest_repair_id=doc["repair_id"],
                repair_states={doc["repair_id"]: "TERMINATED"},
            )

    monkeypatch.setattr(type(deferred.trigger), "sleep", staticmethod(sleep))

    async def first_event() -> Any:
        async for event in deferred.trigger.run():
            return event.payload
        raise AssertionError("no event")

    event = asyncio.run(first_event())
    assert (event["outcome"], event["attempt"]) == ("success", 1)


def test_a_sensor_without_a_snapshot_fails(dbx_project: Any) -> None:
    dag = make_dag(dbx_project)
    ti = FakeTaskInstance(dag_id="d", run_id="r", task_id="a")
    assert sense(dag, ti, "a") == "failed"
    assert sense(dag, ti, naming.FINALIZE_ID) == "failed"
