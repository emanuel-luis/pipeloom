"""DatabricksRunTrigger under asyncio with a scripted FakeDatabricks and a fake clock."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any, ClassVar

import pytest

from pipeloom_airflow._compat import TriggerEvent
from pipeloom_airflow.databricks import triggers
from pipeloom_airflow.databricks.triggers import DatabricksRunTrigger
from pipeloom_databricks.api import ApiError, TaskRunInfo
from pipeloom_databricks.testing import fake as fake_module
from pipeloom_databricks.testing.fake import FakeDatabricks

FACTORY = "pipeloom_databricks.testing.fake:empty"


def make(**kw: Any) -> DatabricksRunTrigger:
    args: dict[str, Any] = {
        "conn_id": "dbx",
        "job_run_id": 900,
        "task_key": None,
        "baseline_attempt": 0,
        "max_attempts": 1,
        "poll_interval": 30.0,
        "client_factory": FACTORY,
    }
    args.update(kw)
    return DatabricksRunTrigger(**args)


async def first_event(trigger: DatabricksRunTrigger) -> dict[str, Any]:
    async for event in trigger.run():
        assert isinstance(event, TriggerEvent)
        payload: dict[str, Any] = event.payload
        return payload
    raise AssertionError("no event")


def task(key: str, result: str | None, life: str = "TERMINATED", attempt: int = 0) -> TaskRunInfo:
    return TaskRunInfo(key, 1, attempt, life, result, None, "m")


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeDatabricks:
    client = FakeDatabricks()
    client.set_run(900, job_id=1, life_cycle_state="RUNNING", tasks=[task("a", None, "RUNNING")])
    monkeypatch.setattr(fake_module, "empty", lambda: client)
    return client


Script = list[Callable[[], None]]


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> tuple[list[float], Script]:
    """Every sleep is recorded and runs the next scripted change of the workspace."""
    slept: list[float] = []
    script: Script = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        if script:
            script.pop(0)()

    monkeypatch.setattr(DatabricksRunTrigger, "sleep", staticmethod(sleep))
    return slept, script


def test_serialize_round_trips_without_secrets() -> None:
    trigger = make(repair_id=5)
    path, kwargs = trigger.serialize()
    assert path == "pipeloom_airflow.databricks.triggers.DatabricksRunTrigger"
    assert DatabricksRunTrigger(**kwargs).serialize() == (path, kwargs)
    assert set(kwargs) == {
        "conn_id",
        "job_run_id",
        "task_key",
        "baseline_attempt",
        "max_attempts",
        "poll_interval",
        "client_factory",
        "repair_id",
    }


def test_shared_mode_waits_for_the_run_and_yields_its_snapshot(
    fake: FakeDatabricks, clock: tuple[list[float], Script]
) -> None:
    slept, script = clock
    script.append(lambda: None)
    script.append(
        lambda: fake.set_run(900, job_id=1, result_state="SUCCESS", tasks=[task("a", "SUCCESS")])
    )
    event = asyncio.run(first_event(make()))
    assert slept == [30.0, 30.0]
    assert event["status"] == "done" and event["job_run_id"] == 900
    assert event["snapshot"]["tasks"]["a"]["result_state"] == "SUCCESS"
    assert fake.called("get_run") == ["900"] * 3


def test_running_twice_gives_the_same_event(
    fake: FakeDatabricks, clock: tuple[list[float], Script]
) -> None:
    fake.set_run(900, job_id=1, result_state="SUCCESS", tasks=[task("a", "SUCCESS")])
    trigger = make()
    assert asyncio.run(first_event(trigger)) == asyncio.run(first_event(trigger))


def test_after_a_repair_the_stale_terminal_state_is_not_the_end(
    fake: FakeDatabricks, clock: tuple[list[float], Script]
) -> None:
    slept, script = clock
    fake.set_run(
        900, job_id=1, result_state="FAILED", tasks=[task("a", "FAILED")], latest_repair_id=4
    )
    script.append(
        lambda: fake.set_run(
            900,
            job_id=1,
            result_state="SUCCESS",
            tasks=[task("a", "SUCCESS", attempt=1)],
            latest_repair_id=5,
            repair_states={5: "TERMINATED"},
        )
    )
    event = asyncio.run(first_event(make(repair_id=5)))
    assert slept == [30.0]
    assert event["snapshot"]["tasks"]["a"]["attempt"] == 1


def test_a_task_key_yields_its_verdict(
    fake: FakeDatabricks, clock: tuple[list[float], Script]
) -> None:
    slept, script = clock
    script.append(
        lambda: fake.set_run(
            900, job_id=1, life_cycle_state="RUNNING", tasks=[task("a", "SUCCESS")]
        )
    )
    event = asyncio.run(first_event(make(task_key="a")))
    assert slept == [30.0]
    assert (event["task_key"], event["outcome"], event["attempt"]) == ("a", "success", 0)


def test_task_verdicts_follow_attempts_and_the_baseline(
    fake: FakeDatabricks, clock: tuple[list[float], Script]
) -> None:
    slept, script = clock
    # attempt 0 failed but Databricks makes a second one: no verdict until it ends
    fake.set_run(900, job_id=1, life_cycle_state="RUNNING", tasks=[task("a", "FAILED", "RUNNING")])
    script.append(
        lambda: fake.set_run(
            900,
            job_id=1,
            life_cycle_state="RUNNING",
            tasks=[task("a", "SUCCESS", attempt=1)],
        )
    )
    event = asyncio.run(first_event(make(task_key="a", max_attempts=2)))
    assert (event["outcome"], event["attempt"], slept) == ("success", 1, [30.0])

    # the last attempt failing is final
    fake.set_run(900, job_id=1, life_cycle_state="RUNNING", tasks=[task("a", "FAILED", attempt=1)])
    event = asyncio.run(first_event(make(task_key="a", max_attempts=2)))
    assert (event["outcome"], event["attempt"]) == ("failed", 1)

    # a stale FAILED below the repair baseline is ignored until the rerun reports
    fake.set_run(900, job_id=1, life_cycle_state="RUNNING", tasks=[task("a", "FAILED", attempt=0)])
    script.append(
        lambda: fake.set_run(
            900, job_id=1, life_cycle_state="RUNNING", tasks=[task("a", "SUCCESS", attempt=1)]
        )
    )
    slept.clear()
    event = asyncio.run(first_event(make(task_key="a", baseline_attempt=1)))
    assert (event["outcome"], event["attempt"], slept) == ("success", 1, [30.0])

    # an excluded task is skipped
    fake.set_run(900, job_id=1, life_cycle_state="RUNNING", tasks=[task("a", "EXCLUDED")])
    event = asyncio.run(first_event(make(task_key="a")))
    assert event["outcome"] == "skipped"


def test_transient_errors_are_retried_with_doubling_sleeps(
    fake: FakeDatabricks, clock: tuple[list[float], Script]
) -> None:
    slept, script = clock
    fake.fail["get_run"] = ApiError(503, "unavailable")
    script.append(lambda: fake.fail.update({"get_run": ApiError(0, "connection reset")}))
    script.append(lambda: fake.fail.pop("get_run"))
    fake.set_run(900, job_id=1, result_state="SUCCESS", tasks=[task("a", "SUCCESS")])
    event = asyncio.run(first_event(make(task_key="a")))
    assert event["outcome"] == "success" and slept == [30.0, 60.0]
    assert fake.called("get_run") == ["900"] * 3


def test_five_transient_errors_in_a_row_are_an_error_event(
    fake: FakeDatabricks, clock: tuple[list[float], Script]
) -> None:
    slept, _ = clock
    fake.fail["get_run"] = ApiError(429, "slow down")
    event = asyncio.run(first_event(make(task_key="a")))
    assert event["status"] == "error" and "HTTP 429" in event["message"]
    assert slept == [30.0, 60.0, 120.0, 120.0, 120.0]
    assert len(fake.called("get_run")) == 6


def test_api_errors_and_bad_connections_are_error_events(
    fake: FakeDatabricks, clock: tuple[list[float], Script], monkeypatch: pytest.MonkeyPatch
) -> None:
    fake.fail["get_run"] = ApiError(403, "denied")
    event = asyncio.run(first_event(make()))
    assert event["status"] == "error" and "PLD009" in event["message"]

    class Conn:
        host = "h.example"
        login = None
        password = None
        extra_dejson: ClassVar[dict[str, Any]] = {}

    async def aget(conn_id: str) -> Conn:
        assert conn_id == "dbx"
        return Conn()

    monkeypatch.setattr(triggers, "aget_connection", aget)
    event = asyncio.run(first_event(make(client_factory=None)))
    assert event["status"] == "error" and "PLD010" in event["message"]
    assert "'dbx'" in event["message"]
