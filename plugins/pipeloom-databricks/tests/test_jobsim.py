"""The job simulator end to end: a rendered job runs through the task wrapper on the memory engine."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import pytest

from pipeloom_databricks.keys import FINALIZE_KEY
from pipeloom_databricks.task_values import TASK_VALUE_MAX_BYTES
from pipeloom_databricks.testing.fake import FakeDatabricks
from pipeloom_databricks.testing.jobsim import SimRun, simulate_job

Sim = Any
OK, BAD, UPSTREAM = "SUCCESS", "FAILED", "UPSTREAM_FAILED"


def test_a_failed_model_fails_finalize_and_a_repair_fixes_the_run(sim: Callable[..., Sim]) -> None:
    s = sim()
    first = simulate_job(s.settings, job_run_id=123, job_parameters={})
    run_id = "dbx-123"  # the pipeloom_run_id parameter is empty
    assert first.task_states == {
        "a": OK,
        "b": OK,
        "c": BAD,
        "d": UPSTREAM,  # run_if ALL_SUCCESS
        "e": OK,
        FINALIZE_KEY: BAD,  # ran although c failed (ALL_DONE), and exits 1
    }
    assert first.exit_codes == {"a": 0, "b": 0, "c": 1, "e": 0, FINALIZE_KEY: 1}

    rows = s.rows(run_id)
    assert {u: r["status"] for u, r in rows.items()} == {
        "a": "succeeded",
        "b": "succeeded",
        "c": "failed",
        "d": "skipped",
        "e": "succeeded",
    }
    assert rows["d"]["skip_reason"] == "upstream_failed"  # derived from the failed c
    ids = rows["a"]["orchestrator_ids"]
    assert ids["databricks_job_id"] == "1"
    assert ids["databricks_job_run_id"] == "123"
    assert ids["databricks_task_key"] == "a"
    assert ids["databricks_task_run_id"].startswith("123")
    run = s.results(run_id)["run"]
    assert (run["run_id"], run["status"]) == (run_id, "failed")
    assert run["identity"]["orchestrator_ids"]["databricks_job_run_id"] == "123"

    values = first.task_values
    assert set(values) == {"a", "b", "c", "e"}  # the finalize task sets none
    assert values["a"]["status"] == "succeeded"
    assert values["a"]["units"]["a"]["attempt"] == 1
    assert values["c"]["status"] == "failed"
    assert values["c"]["units"]["c"]["error_code"]
    assert all(len(json.dumps(v)) <= TASK_VALUE_MAX_BYTES for v in values.values())

    s.make_source_available()
    repair = simulate_job(
        s.settings,
        job_run_id=123,
        job_parameters={},
        rerun={"c", "d", FINALIZE_KEY},
    )
    assert repair.task_states == {"c": OK, "d": OK, FINALIZE_KEY: OK}
    assert s.attempts(run_id, "c") == ["attempt-1.json", "attempt-2.json"]
    assert s.attempts(run_id, "d") == ["attempt-1.json"]
    assert s.attempts(run_id, "a") == ["attempt-1.json"]  # not rerun
    assert repair.task_values["c"]["units"]["c"]["attempt"] == 2
    assert s.results(run_id)["run"]["status"] == "succeeded"
    assert {r["status"] for r in s.rows(run_id).values()} == {"succeeded"}


def test_only_excludes_the_other_tasks_and_finalize_still_runs(sim: Callable[..., Sim]) -> None:
    s = sim()
    res = simulate_job(s.settings, job_run_id=124, job_parameters={}, only={"a", "e", FINALIZE_KEY})
    assert res.task_states == {
        "a": OK,
        "b": "EXCLUDED",
        "c": "EXCLUDED",
        "d": "EXCLUDED",
        "e": OK,
        FINALIZE_KEY: OK,
    }
    rows = s.rows("dbx-124")
    assert {u: r["status"] for u, r in rows.items() if r["status"] != "skipped"} == {
        "a": "succeeded",
        "e": "succeeded",
    }
    assert {r["skip_reason"] for u, r in rows.items() if u in "bcd"} == {"not_run"}
    assert res.exit_codes[FINALIZE_KEY] == 0


def test_a_task_that_dies_without_task_states_counts_as_not_run(sim: Callable[..., Sim]) -> None:
    s = sim()
    res = simulate_job(
        s.settings,
        job_run_id=125,
        job_parameters={},
        only={"a", "e", FINALIZE_KEY},
        fail={"e"},
    )
    assert res.task_states["e"] == BAD and res.exit_codes["e"] == 1
    assert s.attempts("dbx-125", "e") == []
    # no client was given, so finalize could not ask the Jobs API and judges by attempt files
    assert s.rows("dbx-125")["e"]["skip_reason"] == "not_run"
    assert res.exit_codes[FINALIZE_KEY] == 0


def test_a_task_that_dies_fails_the_run_when_finalize_reads_the_task_states(
    sim: Callable[..., Sim],
) -> None:
    s, fake = sim(), FakeDatabricks()
    res = simulate_job(
        s.settings,
        job_run_id=127,
        job_parameters={},
        only={"a", "e", FINALIZE_KEY},
        fail={"e"},
        client=fake,
    )
    assert res.exit_codes[FINALIZE_KEY] == 1
    rows = s.rows("dbx-127")
    assert rows["e"]["status"] == "failed"
    error = rows["e"]["error"]
    assert error["code"] == "PLD011" and "the task failed" in error["message"]
    assert rows["e"]["orchestrator_ids"]["databricks_task_key"] == "e"
    assert set(rows) == {"a", "e"}  # the excluded units get no row
    assert s.results("dbx-127")["run"]["status"] == "failed"
    stored = fake.runs[127]
    assert stored.life_cycle_state == "TERMINATED" and stored.result_state == "FAILED"
    assert {t.task_key: t.result_state for t in stored.tasks} == {
        "a": OK,
        "b": "EXCLUDED",
        "c": "EXCLUDED",
        "d": "EXCLUDED",
        "e": BAD,
        FINALIZE_KEY: BAD,
    }


def test_job_parameters_reach_the_wrapper(sim: Callable[..., Sim]) -> None:
    s = sim()
    res = simulate_job(
        s.settings,
        job_run_id=126,
        job_parameters={"pipeloom_run_id": "nightly-1", "full_refresh": "true"},
        only={"a"},
    )
    assert res.task_states["a"] == OK
    assert s.attempts("nightly-1", "a") == ["attempt-1.json"]
    assert not s.run_dir("dbx-126").exists()


def test_group_members_run_in_order_in_one_task(sim: Callable[..., Sim]) -> None:
    s = sim("by_tag")
    res = simulate_job(s.settings, job_run_id=7, job_parameters={})
    assert res.task_states == {
        "tag_bad": BAD,
        "tag_grp": OK,
        "tag_untagged": OK,
        FINALIZE_KEY: BAD,
    }
    rows = s.rows("dbx-7")
    # b reads the table a wrote: it can only succeed when a ran first
    assert rows["a"]["status"] == rows["b"]["status"] == "succeeded"
    assert rows["a"]["started_at"] <= rows["b"]["started_at"]
    group = res.task_values["tag_grp"]
    assert group["status"] == "succeeded"
    assert set(group["units"]) == {"a", "b"}
    assert rows["a"]["orchestrator_ids"]["databricks_task_key"] == "tag_grp"
    assert rows["c"]["status"] == "failed"
    assert rows["d"]["skip_reason"] == "upstream_failed"


def test_a_cycle_and_an_unsimulated_run_if_are_refused() -> None:
    cyc: dict[str, Any] = {"tasks": [{"task_key": "x", "depends_on": [{"task_key": "x"}]}]}
    with pytest.raises(ValueError, match="cycle"):
        simulate_job(cyc, job_run_id=1, job_parameters={})
    odd: dict[str, Any] = {"tasks": [{"task_key": "x", "run_if": "NONE_FAILED"}]}
    with pytest.raises(ValueError, match="not simulated"):
        simulate_job(odd, job_run_id=1, job_parameters={})


def test_sim_run_defaults() -> None:
    assert SimRun(5) == SimRun(5, {}, {}, {})
