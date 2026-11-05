"""The task wrapper: every flag, the helpers, task values and the entry points."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from types import ModuleType
from typing import Any

import click
import pytest
from click.testing import CliRunner, Result
from pydantic import JsonValue

import pipeloom_databricks
from pipeloom_databricks import task
from pipeloom_databricks.task import (
    check_resolved,
    cli,
    core_argv,
    resolve_run_id,
    started_at,
)
from pipeloom_databricks.task_values import (
    TASK_VALUE_KEY,
    TASK_VALUE_MAX_BYTES,
    runtime_task_values,
    summary,
)

Sim = Any
HASH = "0" * 64


def unit_args(s: Sim, **over: str | None) -> list[str]:
    """The `unit` arguments a rendered job passes, resolved. None drops a flag."""
    given: dict[str, str | None] = {
        "project-dir": s.root.as_posix(),
        "manifest": s.manifest.as_posix(),
        "target": "prod",
        "pipeline": "main",
        "unit": "a",
        "plan-hash": s.plan.metadata.plan_hash,
        "run-id": "r1",
        "full-refresh": "false",
        "job-id": "11",
        "job-run-id": "55",
        "task-key": "a",
        "task-run-id": "66",
        "start-ms": "1700000000000",
    }
    given.update(over)
    return ["unit", *[f"--{k}={v}" for k, v in given.items() if v is not None]]


def invoke(args: list[str], obj: object = None) -> Result:
    return CliRunner().invoke(cli, args, obj=obj)


def test_unit_runs_one_unit_and_exits_with_the_core_code(sim: Callable[..., Sim]) -> None:
    s = sim()
    ok = invoke(unit_args(s, **{"full-refresh": "true"}))
    assert ok.exit_code == 0, ok.stderr
    assert s.attempts("r1", "a") == ["attempt-1.json"]
    assert invoke(unit_args(s, unit="c", **{"task-key": "c"})).exit_code == 1


@pytest.mark.parametrize("pick", [{"unit": None}, {"group": "tag_x"}])
def test_unit_and_group_are_exclusive(sim: Callable[..., Sim], pick: dict[str, str | None]) -> None:
    r = invoke(unit_args(sim(), **pick))
    assert r.exit_code == 2
    assert "exactly one of --unit and --group" in r.stderr


def test_a_non_integer_start_ms_exits_2(sim: Callable[..., Sim]) -> None:
    r = invoke(unit_args(sim(), **{"start-ms": "soon"}))
    assert r.exit_code == 2 and "not an integer" in r.stderr


def test_an_empty_run_id_becomes_dbx_and_the_job_run_id(sim: Callable[..., Sim]) -> None:
    s = sim()
    assert invoke(unit_args(s, **{"run-id": ""})).exit_code == 0
    assert s.attempts("dbx-55", "a") == ["attempt-1.json"]
    r = invoke(unit_args(s, **{"run-id": "", "job-run-id": None}))
    assert r.exit_code == 2 and "--job-run-id is missing" in r.stderr


def test_a_bad_run_id_and_a_bad_full_refresh_exit_2(sim: Callable[..., Sim]) -> None:
    s = sim()
    assert invoke(unit_args(s, **{"run-id": "no spaces"})).exit_code == 2
    r = invoke(unit_args(s, **{"full-refresh": "maybe"}))
    assert r.exit_code == 2 and "true or false" in r.stderr


def test_an_unresolved_reference_is_pld013_and_nothing_runs(sim: Callable[..., Sim]) -> None:
    s = sim()
    r = invoke(unit_args(s, **{"task-key": "{{task.name}}"}))
    assert r.exit_code == 1
    assert "PLD013" in r.stderr and "--task-key" in r.stderr
    assert not s.run_dir("r1").exists()


def test_pld013_honors_the_json_log_format(
    sim: Callable[..., Sim], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PIPELOOM_LOG_FORMAT", "json")
    r = invoke(unit_args(sim(), **{"job-id": "{{job.id}}"}))
    assert json.loads(r.stderr.splitlines()[0])["code"] == "PLD013"


def test_group_runs_its_members_and_an_unknown_group_exits_1(sim: Callable[..., Sim]) -> None:
    s = sim("by_tag")
    assert (
        invoke(unit_args(s, unit=None, group="tag_grp", **{"task-key": "tag_grp"})).exit_code == 0
    )
    assert s.attempts("r1", "a") == s.attempts("r1", "b") == ["attempt-1.json"]
    r = invoke(unit_args(s, unit=None, group="tag_nope"))
    assert r.exit_code == 1 and "no task group 'tag_nope'" in r.stderr
    assert invoke(unit_args(s, unit=None, group="tag_grp", pipeline="nope")).exit_code == 1


def test_finalize_flags_and_exit_code(sim: Callable[..., Sim]) -> None:
    s = sim()
    assert invoke(unit_args(s)).exit_code == 0
    args = unit_args(s, unit=None, **{"task-key": None, "task-run-id": None})
    fin = ["finalize", *[a for a in args[1:]]]
    ok = invoke(fin)
    assert ok.exit_code == 0, ok.stderr
    assert s.results("r1")["run"]["status"] == "succeeded"
    # no connection in a test: one warning, and the run is judged by its attempt files alone
    warned = [ln for ln in ok.stderr.splitlines() if ln.startswith("warning: task states not read")]
    assert len(warned) == 1 and "ConnectionNotConfigured" in warned[0]
    assert invoke(["finalize", *args[1:], "--pipeline=nope"]).exit_code == 1
    assert invoke(unit_args(s, unit="c", **{"task-key": "c"})).exit_code == 1
    assert invoke(fin).exit_code == 1  # c failed: finalize fails the run


def test_task_values_are_published_and_never_change_the_exit_code(
    sim: Callable[..., Sim],
) -> None:
    s = sim()
    seen: dict[str, JsonValue] = {}

    class Recorder:
        def set(self, key: str, value: JsonValue) -> None:
            seen[key] = value

    assert invoke(unit_args(s), obj=task._Context(Recorder())).exit_code == 0
    doc: Any = seen[TASK_VALUE_KEY]
    assert doc["v"] == 1 and doc["run_id"] == "r1" and doc["status"] == "succeeded"
    assert doc["units"]["a"]["attempt"] == 1

    class Broken:
        def set(self, key: str, value: JsonValue) -> None:
            raise RuntimeError("no task values here")

    r = invoke(unit_args(s, **{"run-id": "r2"}), obj=task._Context(Broken()))
    assert r.exit_code == 0
    assert "task value not set: RuntimeError" in r.stderr


def test_main_exits_only_on_failure_and_the_lazy_entry_reads_argv(
    sim: Callable[..., Sim], monkeypatch: pytest.MonkeyPatch
) -> None:
    s = sim()
    with pytest.raises(SystemExit) as stop:
        task.main(unit_args(s, unit="c", **{"task-key": "c"}))
    assert stop.value.code == 1
    monkeypatch.setattr(sys, "argv", ["pipeloom_databricks_task", *unit_args(s)])
    # success returns: serverless (IPython) reports even SystemExit(0) as a failed task
    pipeloom_databricks.pipeloom_databricks_task()


# -- helpers -------------------------------------------------------------------------------------


def test_resolve_run_id() -> None:
    assert resolve_run_id("", "123") == "dbx-123"
    assert resolve_run_id("nightly.1", None) == "nightly.1"
    with pytest.raises(click.UsageError):
        resolve_run_id("", None)
    with pytest.raises(click.BadParameter):
        resolve_run_id("x" * 65, "1")


def test_check_resolved(capsys: pytest.CaptureFixture[str]) -> None:
    check_resolved({"a": "x", "b": ""})
    with pytest.raises(SystemExit) as stop:
        check_resolved({"run-id": "{{job.parameters.pipeloom_run_id}}", "ok": "1"})
    assert stop.value.code == 1
    err = capsys.readouterr().err
    assert "PLD013" in err and "--run-id" in err and "--ok" not in err


def test_started_at_is_exact_utc() -> None:
    assert started_at("1700000000123") == "2023-11-14T22:13:20.123+00:00"
    with pytest.raises(click.BadParameter):
        started_at("")


def test_core_argv() -> None:
    assert core_argv(
        "run",
        project_dir="/p",
        target="prod",
        manifest="/p/m.json",
        plan_hash=HASH,
        run_id="r",
        run_started_at="2023-11-14T22:13:20.000+00:00",
        full_refresh=True,
        select=["unit:a", "unit:b"],
        ids={"job_id": "1", "task_key": ""},
    ) == [
        "run",
        "--project-dir=/p",
        "--target=prod",
        "--manifest=/p/m.json",
        f"--expect-plan-hash={HASH}",
        "--run-id=r",
        "--run-started-at=2023-11-14T22:13:20.000+00:00",
        "--full-refresh=true",
        "--select=unit:a",
        "--select=unit:b",
        "--orchestrator-id=databricks_job_id=1",
    ]
    fin = core_argv(
        "finalize",
        project_dir="/p",
        target="t",
        manifest="m",
        plan_hash=HASH,
        run_id="r",
        run_started_at="x",
    )
    assert not any(a.startswith("--full-refresh") for a in fin)


# -- task values ---------------------------------------------------------------------------------


def _row(unit: str) -> dict[str, JsonValue]:
    return {
        "unit_id": unit,
        "status": "succeeded",
        "attempt": 1,
        "rows_written": 10,
        "duration_ms": 5,
        "target_version": "3",
        "error": None,
        "warnings": ["w1", "w2"],
    }


def test_summary_shape_and_truncation() -> None:
    small: Any = summary("r", "succeeded", [_row("b"), _row("a")])
    assert list(small["units"]) == ["a", "b"]
    assert small["units"]["a"] == {
        "status": "succeeded",
        "attempt": 1,
        "rows_written": 10,
        "duration_ms": 5,
        "target_version": "3",
        "error_code": None,
        "warnings": 2,
    }
    assert "truncated" not in small

    rows = [_row(f"unit_{i:04d}") for i in range(600)]
    big: Any = summary("r", "failed", rows)
    assert big["truncated"] is True
    kept = list(big["units"])
    assert 0 < len(kept) < 600 and kept == sorted(kept)
    assert kept == [f"unit_{i:04d}" for i in range(len(kept))]  # dropped from the end
    assert len(json.dumps(big, separators=(",", ":"))) <= TASK_VALUE_MAX_BYTES


def test_runtime_task_values_need_a_databricks_runtime(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert runtime_task_values() is None
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4")
    marker = object()
    names = ("databricks", "databricks.sdk", "databricks.sdk.runtime")
    mods = {n: ModuleType(n) for n in names}
    mods["databricks.sdk.runtime"].dbutils = type(  # type: ignore[attr-defined]
        "D", (), {"jobs": type("J", (), {"taskValues": marker})}
    )
    for n, m in mods.items():
        monkeypatch.setitem(sys.modules, n, m)
    assert runtime_task_values() is marker
    monkeypatch.delitem(sys.modules, "databricks.sdk.runtime")
    monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", None)  # import fails
    assert runtime_task_values() is None
