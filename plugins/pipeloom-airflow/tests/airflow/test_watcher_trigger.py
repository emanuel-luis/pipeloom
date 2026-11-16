"""PipeloomWatcherTrigger under asyncio, over attempt files in tmp_path, with a fake clock."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from pipeloom_airflow._compat import TriggerEvent
from pipeloom_airflow.watcher.state import VISIBILITY_GRACE_S
from pipeloom_airflow.watcher.triggers import PipeloomWatcherTrigger

Script = list[Callable[[], None]]


def make(run_dir: Path, **kw: Any) -> PipeloomWatcherTrigger:
    args: dict[str, Any] = {
        "run_dir": run_dir.as_posix(),
        "unit_id": "u",
        "retries": 1,
        "retry_on": "transient",
        "poll_interval": 10.0,
        "recheck_after": 300.0,
    }
    args.update(kw)
    return PipeloomWatcherTrigger(**args)


def attempt(run_dir: Path, n: int, status: str, category: str = "transient") -> None:
    error = (
        None
        if status == "succeeded"
        else {"code": "PLM204", "message": "boom", "category": category}
    )
    path = run_dir / "units" / "u" / f"attempt-{n}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"unit_id": "u", "status": status, "retries_used": n - 1, "error": error}
    path.write_text(json.dumps(row), encoding="utf-8")


def results(run_dir: Path, rows: list[dict[str, Any]]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run_results.json").write_text(json.dumps({"results": rows}), encoding="utf-8")


async def first_event(trigger: PipeloomWatcherTrigger) -> dict[str, Any]:
    async for event in trigger.run():
        assert isinstance(event, TriggerEvent)
        payload: dict[str, Any] = event.payload
        return payload
    raise AssertionError("no event")


@pytest.fixture
def run_dir(tmp_path: Path) -> Path:
    return tmp_path / "runs" / "r1"


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> tuple[list[float], Script]:
    """Every sleep advances the fake clock and runs the next scripted change of the files."""
    slept: list[float] = []
    script: Script = []
    now = [1000.0]

    async def sleep(seconds: float) -> None:
        slept.append(seconds)
        now[0] += seconds
        if script:
            script.pop(0)()

    monkeypatch.setattr(PipeloomWatcherTrigger, "sleep", staticmethod(sleep))
    monkeypatch.setattr(PipeloomWatcherTrigger, "monotonic", staticmethod(lambda: now[0]))
    return slept, script


def test_serialize_round_trips() -> None:
    trigger = make(Path("/srv/root/runs/r1"))
    path, kwargs = trigger.serialize()
    assert path == "pipeloom_airflow.watcher.triggers.PipeloomWatcherTrigger"
    assert PipeloomWatcherTrigger(**kwargs).serialize() == (path, kwargs)
    assert set(kwargs) == {
        "run_dir",
        "unit_id",
        "retries",
        "retry_on",
        "poll_interval",
        "recheck_after",
    }


def test_a_success_written_later_is_done(run_dir: Path, clock: tuple[list[float], Script]) -> None:
    slept, script = clock
    script.append(lambda: attempt(run_dir, 1, "failed"))  # transient, one retry left: wait
    script.append(lambda: attempt(run_dir, 2, "succeeded"))
    event = asyncio.run(first_event(make(run_dir)))
    assert slept == [10.0, 10.0]
    assert (event["status"], event["outcome"], event["attempt"]) == ("done", "success", 2)


def test_a_final_failure_is_done_with_its_code(
    run_dir: Path, clock: tuple[list[float], Script]
) -> None:
    attempt(run_dir, 1, "failed", category="permanent")
    event = asyncio.run(first_event(make(run_dir)))
    assert (event["status"], event["outcome"], event["code"], event["message"]) == (
        "done",
        "failed",
        "PLM204",
        "boom",
    )
    assert clock[0] == []


def test_a_skipped_row_is_run(run_dir: Path, clock: tuple[list[float], Script]) -> None:
    results(run_dir, [{"unit_id": "u", "status": "skipped"}])
    assert asyncio.run(first_event(make(run_dir)))["status"] == "run"


def test_an_unseen_verdict_is_invisible_after_the_grace(
    run_dir: Path, clock: tuple[list[float], Script]
) -> None:
    slept, _ = clock
    results(run_dir, [{"unit_id": "u", "status": "succeeded", "attempt": 1}])
    event = asyncio.run(first_event(make(run_dir)))
    assert (event["status"], event["code"]) == ("invisible", "PLA004")
    assert sum(slept) >= VISIBILITY_GRACE_S and sum(slept) < VISIBILITY_GRACE_S + 10.0


def test_a_file_showing_within_the_grace_is_done(
    run_dir: Path, clock: tuple[list[float], Script]
) -> None:
    _, script = clock
    results(run_dir, [{"unit_id": "u", "status": "succeeded", "attempt": 1}])
    script.extend([lambda: None, lambda: attempt(run_dir, 1, "succeeded")])
    event = asyncio.run(first_event(make(run_dir)))
    assert (event["status"], event["outcome"]) == ("done", "success")


def test_nothing_final_is_pending_after_the_recheck(
    run_dir: Path, clock: tuple[list[float], Script]
) -> None:
    slept, _ = clock
    event = asyncio.run(first_event(make(run_dir, recheck_after=35.0)))
    assert event["status"] == "pending" and slept == [10.0, 10.0, 10.0, 10.0]


def test_running_twice_gives_the_same_event(
    run_dir: Path, clock: tuple[list[float], Script]
) -> None:
    attempt(run_dir, 1, "succeeded")
    trigger = make(run_dir)
    assert asyncio.run(first_event(trigger)) == asyncio.run(first_event(trigger))
