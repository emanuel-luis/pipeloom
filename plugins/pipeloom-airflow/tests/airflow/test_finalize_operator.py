"""PipeloomFinalizeOperator: Airflow task states plus attempt files give the run's outcome."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeloom.artifacts.layout import RunPaths
from pipeloom_airflow import naming
from pipeloom_airflow._compat import AirflowException, AirflowFailException
from pipeloom_airflow.operators import PipeloomFinalizeOperator, PipeloomRunOperator
from pipeloom_airflow.testing.context import FakeTaskInstance, make_context

AFTER = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
Project = tuple[Path, Path, str]
Plan = tuple[Any, Any]

RID = naming.airflow_run_id("d", "manual__1")


def run_units(af_plan: Plan, env: dict[str, str]) -> dict[str, str | None]:
    """Run every unit like the DAG would and return the Airflow task states."""
    plan, command = af_plan
    states: dict[str, str | None] = {}
    for t in plan.tasks:  # sorted by id; base runs before child
        if any(states.get(u) == "failed" for u in t.upstream):
            states[t.task_id] = "upstream_failed"
            continue
        op = PipeloomRunOperator(
            task_id=t.task_id,
            unit_id=t.unit_id,
            command=command,
            venv_dir=None,
            env=env,
            retry_on=t.retry_on,
            cancel_grace_s=plan.cancel_grace_s,
            group_id=None,
        )
        ti = FakeTaskInstance(dag_id="d", run_id="manual__1", task_id=t.task_id)
        try:
            op.execute(make_context(ti, run_after=AFTER, params={}))
            states[t.task_id] = "success"
        except AirflowException:
            states[t.task_id] = "failed"
    return states


def finalize(af_plan: Plan, states: dict[str, str | None]) -> None:
    plan, command = af_plan
    op = PipeloomFinalizeOperator(
        task_id=naming.FINALIZE_ID,
        command=command,
        venv_dir=None,
        env={},
        task_units={t.task_id: t.units for t in plan.tasks},
        select=plan.select,
        exclude=plan.exclude,
        group_id=None,
    )
    assert op.trigger_rule == "all_done" and op.retries == 1
    ti = FakeTaskInstance(dag_id="d", run_id="manual__1", task_id=op.task_id, states=states)
    op.execute(make_context(ti, run_after=AFTER, params={}))


def results(af_project: Project) -> dict[str, Any]:
    path = RunPaths(af_project[0] / "target", RID).run_results
    data: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return data


def statuses(af_project: Project) -> dict[str, str]:
    return {r["unit_id"]: r["status"] for r in results(af_project)["results"]}


def test_a_successful_run_finalizes(af_project: Project, af_plan: Plan) -> None:
    states = run_units(af_plan, {})
    assert set(states.values()) == {"success"}
    finalize(af_plan, states)
    assert set(statuses(af_project).values()) == {"succeeded"}


def test_a_failed_unit_fails_finalize(af_project: Project, af_plan: Plan) -> None:
    states = run_units(af_plan, {"AFPROJ_MODE": "permanent"})
    with pytest.raises(AirflowFailException, match=f"pipeloom run {RID} failed"):
        finalize(af_plan, states)
    assert "failed" in statuses(af_project).values()


def test_a_success_without_an_attempt_file_is_pla004(af_project: Project, af_plan: Plan) -> None:
    states = run_units(af_plan, {})
    plan, _ = af_plan
    unit = next(t.unit_id for t in plan.tasks if t.models == ("base",))
    paths = RunPaths(af_project[0] / "target", RID)
    assert paths.attempt_numbers(unit) == [1]
    paths.attempt(unit, 1).unlink()
    with pytest.raises(AirflowFailException, match=f"pipeloom run {RID} failed"):
        finalize(af_plan, states)
    rows = {r["unit_id"]: r for r in results(af_project)["results"]}
    assert rows[unit]["status"] == "failed" and rows[unit]["error"]["code"] == "PLA004"
    assert len(rows) == len(plan.tasks)


def test_finalize_without_a_virtualenv_fails(af_plan: Plan, tmp_path: Path) -> None:
    _, command = af_plan
    op = PipeloomFinalizeOperator(
        task_id=naming.FINALIZE_ID,
        command=command,
        venv_dir=(tmp_path / "nope").as_posix(),
        env={},
        task_units={},
        select=(),
        exclude=(),
        group_id=None,
    )
    ti = FakeTaskInstance(dag_id="d", run_id="manual__1", task_id=op.task_id)
    with pytest.raises(AirflowFailException, match="PLA002"):
        op.execute(make_context(ti, run_after=AFTER, params={}))
