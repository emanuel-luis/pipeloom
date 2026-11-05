"""Task states to unit states: the mapping table, the file core finalize reads, and the wrapper."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from pipeloom.runtime.finalize_run import ExternalUnitState, read_unit_states
from pipeloom_databricks.client import ApiError, RunInfo, TaskRunInfo
from pipeloom_databricks.finalize_states import task_states, unit_states, unit_states_json
from pipeloom_databricks.task import main
from pipeloom_databricks.testing.fake import FakeDatabricks
from pipeloom_databricks.testing.jobsim import simulate_job

Sim = Any


def task(
    key: str, result: str | None, *, message: str | None = None, code: str | None = None
) -> TaskRunInfo:
    return TaskRunInfo(key, 900, 1, "TERMINATED", result, code, message)


def finalize_argv(s: Sim, fake: FakeDatabricks, *, job_run_id: str, run_id: str) -> int:
    """Run the finalize task of the sim job against `fake`; its exit code."""
    argv = [
        "finalize",
        f"--project-dir={s.root.as_posix()}",
        f"--manifest={s.manifest.as_posix()}",
        "--target=prod",
        "--pipeline=main",
        f"--plan-hash={s.plan.metadata.plan_hash}",
        f"--run-id={run_id}",
        "--full-refresh=false",
        "--job-id=1",
        f"--job-run-id={job_run_id}",
        "--start-ms=1700000000000",
    ]
    try:
        main(argv, client_factory=lambda: fake)
    except SystemExit as stop:
        assert isinstance(stop.code, int)
        return stop.code
    return 0


def run_of(*tasks: TaskRunInfo) -> RunInfo:
    return RunInfo(55, 1, "TERMINATED", "FAILED", tasks=tasks)


KEYS = {"a": "ta", "b": "tb", "c": "tc"}


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        ("EXCLUDED", ExternalUnitState("excluded")),
        ("UPSTREAM_FAILED", "skipped"),
        ("UPSTREAM_CANCELED", "skipped"),
        ("CANCELED", "cancelled"),
        ("TIMEDOUT", "cancelled"),
        ("FAILED", "failed"),
        ("SUCCESS", None),
        ("SUCCESS_WITH_FAILURES", None),
        ("MAXIMUM_CONCURRENT_RUNS_REACHED", None),
        (None, None),
    ],
)
def test_mapping_table(result: str | None, expected: ExternalUnitState | str | None) -> None:
    got = unit_states(KEYS, run_of(task("ta", result, message="boom")), set())
    if expected is None:
        assert got == {}
        return
    state = got["a"]
    assert state.status == (expected if isinstance(expected, str) else expected.status)
    if state.status == "excluded":
        assert state.orchestrator_ids == {}
    else:
        assert state.orchestrator_ids == {
            "databricks_task_key": "ta",
            "databricks_task_run_id": "900",
        }
    if state.status == "skipped":
        assert state.reason is None  # upstream_failed
    if state.status == "failed":
        assert state.code == "PLD011"
        assert state.message == "task ta ended without a unit result: boom"


def test_failed_without_a_message_names_the_termination_code() -> None:
    got = unit_states(KEYS, run_of(task("ta", "FAILED", code="RUN_EXECUTION_ERROR")), set())
    assert got["a"].message is not None and "RUN_EXECUTION_ERROR" in got["a"].message
    bare = unit_states(KEYS, run_of(task("ta", "FAILED")), set())
    assert bare["a"].message is not None and "no reason given" in bare["a"].message


def test_a_unit_with_an_attempt_a_missing_task_or_a_shared_task() -> None:
    run = run_of(task("ta", "FAILED"), task("tb", "CANCELED"))
    got = unit_states(KEYS, run, {"a"})  # a wrote an attempt: its own file wins
    assert sorted(got) == ["b"]  # c has no task in the run: no entry
    grouped = unit_states({"a": "g", "b": "g"}, run_of(task("g", "TIMEDOUT")), {"a"})
    assert sorted(grouped) == ["b"] and grouped["b"].status == "cancelled"


def test_task_states_by_key_and_client_errors_pass_through() -> None:
    fake = FakeDatabricks()
    fake.set_run(55, job_id=1, tasks=[task("ta", "SUCCESS"), task("tb", "FAILED")])
    assert sorted(task_states(fake, 55)) == ["ta", "tb"]
    assert task_states(fake, 55)["tb"].result_state == "FAILED"
    with pytest.raises(ApiError):
        task_states(fake, 404)


def test_the_file_is_what_core_finalize_reads(tmp_path: Path) -> None:
    states = unit_states(
        {"a": "ta", "b": "tb", "c": "tc", "d": "td"},
        run_of(
            task("ta", "FAILED", message="café lost"),
            task("tb", "CANCELED"),
            task("tc", "UPSTREAM_FAILED"),
            task("td", "EXCLUDED"),
        ),
        set(),
    )
    data = unit_states_json(states)
    assert data.isascii() and data.endswith(b"\n")
    path = tmp_path / "s.json"
    path.write_bytes(data)
    again = read_unit_states(path)
    assert again == states
    doc = json.loads(data)
    assert doc["units"]["d"] == {"status": "excluded"}


def test_the_wrapper_passes_states_to_core_and_a_cancelled_task_cancels_the_run(
    sim: Callable[..., Sim],
) -> None:
    s, fake = sim(), FakeDatabricks()
    fake.set_run(
        61,
        job_id=1,
        tasks=[task("a", "SUCCESS"), task("e", "TIMEDOUT")],
    )
    # a wrote an attempt of its own; every other task of the run is missing or cancelled
    assert simulate_job(s.settings, job_run_id=62, job_parameters={}, only={"a"}).exit_codes == {
        "a": 0
    }
    code = finalize_argv(s, fake, job_run_id="61", run_id="dbx-62")
    assert code == 130
    rows = s.rows("dbx-62")
    assert rows["e"]["status"] == "cancelled"
    assert rows["a"]["status"] == "succeeded"
    assert s.results("dbx-62")["run"]["status"] == "cancelled"


def test_a_client_error_is_one_warning_and_no_states(
    sim: Callable[..., Sim], capsys: pytest.CaptureFixture[str]
) -> None:
    s, fake = sim(), FakeDatabricks()
    fake.fail["get_run"] = ApiError(500, "the service is down", "INTERNAL_ERROR")
    simulate_job(s.settings, job_run_id=63, job_parameters={}, only={"a"})
    assert finalize_argv(s, fake, job_run_id="63", run_id="dbx-63") == 0
    err = capsys.readouterr().err
    assert err.count("warning: task states not read") == 1
    assert "ApiError" in err and "the service is down" in err
    assert s.rows("dbx-63")["e"]["skip_reason"] == "not_run"


def test_a_one_time_run_has_no_job_run_id_to_ask_about(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    simulate_job(s.settings, job_run_id=64, job_parameters={"pipeloom_run_id": "once"}, only={"a"})
    assert finalize_argv(s, fake, job_run_id="", run_id="once") == 0
    assert fake.calls == []
    # the unit rows exist (not_run), the states were never asked for
    assert s.rows("once")["e"]["skip_reason"] == "not_run"
