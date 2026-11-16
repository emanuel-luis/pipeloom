"""PipeloomWatcherSensor with a fake task context: every branch of the watcher state machine
(ADR-0048, rule 6), deferring again from `execute_complete`, and reruns with a real child."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from pipeloom.artifacts.layout import RunPaths
from pipeloom_airflow import naming
from pipeloom_airflow._compat import AirflowException, AirflowFailException
from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, ProjectConfig
from pipeloom_airflow.dag import PipeloomDag
from pipeloom_airflow.testing.context import FakeTaskInstance, make_context
from pipeloom_airflow.watcher.state import RECHECK_S, VISIBILITY_GRACE_S

AFTER = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
RUN_ID = "manual__1"
Project = tuple[Path, Path, str]


def watcher(af_project: Project, model: str = "base", **execution: Any) -> Any:
    project, manifest, _ = af_project
    dag = PipeloomDag(
        dag_id="d",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        project_config=ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
        target="dev",
        execution_config=ExecutionConfig(
            mode=ExecutionMode.WATCHER, watcher_timeout=3600.0, **execution
        ),
    )
    tasks: list[Any] = list(dag.tasks)
    return next(
        t for t in tasks if type(t).__name__ == "PipeloomWatcherSensor" and model in t.unit_id
    )


def paths(af_project: Project) -> RunPaths:
    return RunPaths(af_project[0] / "target", naming.airflow_run_id("d", RUN_ID))


def write_attempt(af_project: Project, unit_id: str, n: int, status: str, **error: Any) -> None:
    path = paths(af_project).attempt(unit_id, n)
    path.parent.mkdir(parents=True, exist_ok=True)
    err = None
    if status != "succeeded":
        err = {"code": "PLM204", "message": "boom", "category": "permanent", **error}
    row = {"unit_id": unit_id, "status": status, "retries_used": n - 1, "error": err}
    path.write_text(json.dumps(row), encoding="utf-8")


def write_results(af_project: Project, rows: list[dict[str, Any]]) -> None:
    path = paths(af_project).run_results
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"results": rows}), encoding="utf-8")


def context(op: Any, producer: str | None, try_number: int = 1) -> dict[str, Any]:
    ti = FakeTaskInstance(
        dag_id="d",
        run_id=RUN_ID,
        task_id=op.task_id,
        try_number=try_number,
        states={naming.PRODUCER_ID: producer},
    )
    return make_context(ti, run_after=AFTER, params={})


def deferred(call: Any) -> Any:
    with pytest.raises(BaseException) as info:  # TaskDeferred is not an Exception
        call()
    assert type(info.value).__name__ == "TaskDeferred", info.value
    return info.value


def test_an_open_unit_defers_on_its_trigger(af_project: Project) -> None:
    op = watcher(af_project)
    before = time.time()
    d = deferred(lambda: op.execute(context(op, "running")))
    assert d.method_name == "execute_complete"
    assert d.kwargs["grace"] is False
    assert before + 3600 <= d.kwargs["deadline"] <= time.time() + 3600
    assert 3590 < d.timeout.total_seconds() <= 3600
    _, kwargs = d.trigger.serialize()
    assert kwargs == {
        "run_dir": paths(af_project).run_dir.as_posix(),
        "unit_id": op.unit_id,
        "retries": op.retries,
        "retry_on": op.retry_on,
        "poll_interval": 10.0,
        "recheck_after": RECHECK_S,
    }


def capture_log(op: Any, monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace the operator's task logger with a mock (the Airflow logger's handlers vary)."""
    log = MagicMock()
    monkeypatch.setattr(type(op), "log", log, raising=False)
    return log


def logged(log: MagicMock) -> str:
    """Every message the mock received, formatted like the logging module does."""
    calls = [*log.info.call_args_list, *log.error.call_args_list]
    return "\n".join(c.args[0] % c.args[1:] for c in calls)


def test_a_succeeded_attempt_succeeds_and_logs_its_tail(
    af_project: Project, monkeypatch: pytest.MonkeyPatch
) -> None:
    op = watcher(af_project)
    log = capture_log(op, monkeypatch)
    write_attempt(af_project, op.unit_id, 1, "succeeded")
    path = paths(af_project).log(op.unit_id, 1)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"line {i}\n" for i in range(300)), encoding="utf-8")
    assert op.execute(context(op, "running")) is None
    assert op.execute(context(op, "success")) is None
    text = logged(log)
    assert f"unit {op.unit_id} attempt 1 succeeded" in text
    assert "line 299" in text
    assert "line 99\n" not in text  # only the last 200 of 300 lines
    assert "last 200 lines" in text


def test_a_succeeded_attempt_without_a_unit_log_points_at_the_producer(
    af_project: Project, monkeypatch: pytest.MonkeyPatch
) -> None:
    op = watcher(af_project)
    log = capture_log(op, monkeypatch)
    write_attempt(af_project, op.unit_id, 1, "succeeded")
    assert op.execute(context(op, "success")) is None
    text = logged(log)
    assert f"its output is in the log of task {naming.PRODUCER_ID}" in text
    assert "no log at" not in text


def test_a_reported_failure_fails_without_retry(
    af_project: Project, monkeypatch: pytest.MonkeyPatch
) -> None:
    op = watcher(af_project)
    log = capture_log(op, monkeypatch)
    write_attempt(af_project, op.unit_id, 1, "failed", trace_excerpt=["Traceback", "ValueError"])
    with pytest.raises(AirflowFailException, match="PLM204 boom"):
        op.execute(context(op, "success"))
    text = logged(log)
    assert "trace excerpt:\nTraceback\nValueError" in text


def test_a_cancelled_row_fails(af_project: Project) -> None:
    op = watcher(af_project)
    write_results(af_project, [{"unit_id": op.unit_id, "status": "cancelled", "error": None}])
    with pytest.raises(AirflowFailException, match="cancelled"):
        op.execute(context(op, "success"))


def test_an_ended_producer_without_results_is_pla009_after_the_grace(
    af_project: Project,
) -> None:
    op = watcher(af_project)
    d = deferred(lambda: op.execute(context(op, "failed")))
    assert d.kwargs["grace"] is True
    assert d.trigger.serialize()[1]["recheck_after"] == VISIBILITY_GRACE_S
    pending = {"v": 1, "status": "pending", "unit_id": op.unit_id}
    with pytest.raises(AirflowException, match="PLA009") as info:
        op.execute_complete(context(op, "failed"), pending, **d.kwargs)
    assert not isinstance(info.value, AirflowFailException)


def test_pending_defers_again_with_the_remaining_deadline(af_project: Project) -> None:
    op = watcher(af_project)
    pending = {"v": 1, "status": "pending", "unit_id": op.unit_id}
    deadline = time.time() + 100.0
    d = deferred(lambda: op.execute_complete(context(op, "running"), pending, deadline, False))
    assert d.kwargs == {"deadline": deadline, "grace": False}
    assert d.timeout.total_seconds() <= 100.0
    assert d.trigger.serialize()[1]["recheck_after"] <= 100.0
    # the files decide on resume, whatever the event said
    write_attempt(af_project, op.unit_id, 1, "succeeded")
    assert op.execute_complete(context(op, "running"), pending, deadline, False) is None


def test_a_passed_deadline_fails_the_try(af_project: Project) -> None:
    op = watcher(af_project)
    pending = {"v": 1, "status": "pending", "unit_id": op.unit_id}
    with pytest.raises(AirflowException, match="watcher_timeout") as info:
        op.execute_complete(context(op, "running"), pending, time.time() - 1.0, False)
    assert not isinstance(info.value, AirflowFailException)


def test_an_invisible_event_is_pla004(af_project: Project) -> None:
    op = watcher(af_project)
    event = {"v": 1, "status": "invisible", "code": "PLA004", "message": "not visible"}
    with pytest.raises(AirflowException, match="PLA004 not visible"):
        op.execute_complete(context(op, "success"), event, time.time() + 100.0, False)


def test_a_cleared_watcher_waits_while_the_producer_runs_its_unit(af_project: Project) -> None:
    op = watcher(af_project)
    deferred(lambda: op.execute(context(op, "running", try_number=2)))
    assert paths(af_project).attempt_numbers(op.unit_id) == []


def test_a_cleared_watcher_reruns_its_unit_once_the_producer_is_done(
    af_project: Project,
) -> None:
    op = watcher(af_project)
    write_attempt(af_project, op.unit_id, 1, "succeeded")
    assert op.execute(context(op, "success", try_number=2)) is None
    assert paths(af_project).attempt_numbers(op.unit_id) == [1, 2]
    row = json.loads(paths(af_project).attempt(op.unit_id, 2).read_text(encoding="utf-8"))
    assert row["status"] == "succeeded"


def test_a_skipped_row_runs_the_unit_here(af_project: Project) -> None:
    op = watcher(af_project)
    write_results(af_project, [{"unit_id": op.unit_id, "status": "skipped"}])
    assert op.execute(context(op, "success")) is None
    assert paths(af_project).attempt_numbers(op.unit_id) == [1]


def test_a_rerun_maps_exit_codes_as_local(af_project: Project) -> None:
    op = watcher(af_project, "other", env={"AFPROJ_MODE": "permanent"})
    with pytest.raises(AirflowFailException, match="bad data"):
        op.execute(context(op, "success", try_number=2))
    op = watcher(af_project, "other", env={"AFPROJ_MODE": "transient"})
    with pytest.raises(AirflowException, match="network blip") as info:
        op.execute(context(op, "success", try_number=3))
    assert not isinstance(info.value, AirflowFailException)
