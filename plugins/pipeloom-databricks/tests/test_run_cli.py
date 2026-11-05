"""`pipeloom databricks run` and `repair`: every flag, on a fake workspace."""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from click.testing import CliRunner, Result

from pipeloom_databricks.cli import Hooks, databricks
from pipeloom_databricks.client import TaskRunInfo
from pipeloom_databricks.keys import FINALIZE_KEY
from pipeloom_databricks.testing.fake import FakeDatabricks

Sim = Any
FACTORY = ("--client-factory", "fakemod:make")


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeDatabricks:
    client = FakeDatabricks()
    monkeypatch.setitem(sys.modules, "fakemod", SimpleNamespace(make=lambda: client))
    return client


def invoke(
    s: Sim, command: str, *args: str, sleep: Callable[[float], None] | None = None
) -> Result:
    argv = [*FACTORY, command, "--project-dir", str(s.root), "--target", "prod", *args]
    return CliRunner().invoke(databricks, argv, obj=Hooks(sleep) if sleep else None)


def first_line(result: Result) -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(result.stdout.splitlines()[0])
    return doc


def ending(fake: FakeDatabricks, result: str, *tasks: TaskRunInfo) -> Callable[[float], None]:
    """A sleep that ends every run of the fake with `result`."""

    def sleep(seconds: float) -> None:
        for rid, run in list(fake.runs.items()):
            fake.runs[rid] = replace(
                run, life_cycle_state="TERMINATED", result_state=result, tasks=tasks
            )

    return sleep


def task(key: str, result: str) -> TaskRunInfo:
    return TaskRunInfo(key, 1, 1, "TERMINATED", result, None, "it broke")


def test_run_ensure_starts_the_job_and_no_wait_prints_one_json_line(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    r = invoke(s, "run", "--pipeline", "main", "--ensure", "--no-wait")
    assert r.exit_code == 0, r.stderr
    doc = first_line(r)
    assert sorted(doc) == ["job_id", "job_run_id", "run_id", "run_page_url"]
    assert doc["run_id"] == f"dbx-{doc['job_run_id']}" and doc["job_id"] in fake.jobs
    assert r.stdout.isascii() and len(r.stdout.splitlines()) == 1


def test_missing_job_is_pld007_and_stale_job_is_pld008(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    r = invoke(s, "run", "--pipeline", "main", "--no-wait")
    assert r.exit_code == 1 and "PLD007" in r.stderr and r.stdout == ""
    invoke(s, "run", "--pipeline", "main", "--ensure", "--no-wait")
    (job_id,) = fake.jobs
    tags: Any = fake.jobs[job_id]["tags"]
    tags["pipeloom_settings_hash"] = "0" * 64
    r = invoke(s, "run", "--pipeline", "main", "--no-wait")
    assert r.exit_code == 1 and "PLD008" in r.stderr


def test_pipeline_is_required_when_there_are_several_and_must_exist(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    r = invoke(s, "run", "--no-wait")
    assert r.exit_code == 2 and "--pipeline is required" in r.stderr
    r = invoke(s, "run", "--pipeline", "nope", "--no-wait")
    assert r.exit_code == 2 and "not in the deploy block" in r.stderr
    assert fake.calls == []


def test_run_id_full_refresh_select_and_exclude_reach_the_start(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    r = invoke(
        s,
        "run",
        "--pipeline",
        "main",
        "--ensure",
        "--no-wait",
        "--run-id",
        "nightly-1",
        "--full-refresh",
        "--select",
        "a",
        "--select",
        "e",
        "--exclude",
        "e",
    )
    assert r.exit_code == 0, r.stderr
    assert first_line(r)["run_id"] == "nightly-1"
    ((_, params, only, token),) = fake.starts
    assert params == {"full_refresh": "true", "pipeloom_run_id": "nightly-1"}
    assert only == ("a", FINALIZE_KEY) and token is not None
    short = invoke(s, "run", "--pipeline", "main", "--no-wait", "-s", "c")
    assert short.exit_code == 0 and fake.starts[-1][2] == ("c", FINALIZE_KEY)


def test_bad_run_id_and_empty_selection_exit_2(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    r = invoke(s, "run", "--pipeline", "main", "--run-id", "no spaces", "--ensure")
    assert r.exit_code == 2 and "--run-id" in r.stderr
    r = invoke(s, "run", "--pipeline", "extra", "--ensure", "--no-wait", "--select", "a")
    assert r.exit_code == 2 and "no unit" in r.stderr
    assert fake.starts == []


def test_ephemeral_submits_and_rejects_ensure(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    r = invoke(s, "run", "--pipeline", "main", "--ephemeral", "--no-wait", "--run-id", "once")
    assert r.exit_code == 0, r.stderr
    doc = first_line(r)
    assert doc["job_id"] is None and doc["run_id"] == "once"
    assert len(fake.submissions) == 1 and fake.jobs == {}
    r = invoke(s, "run", "--pipeline", "main", "--ephemeral", "--ensure")
    assert r.exit_code == 2 and "--ensure" in r.stderr


def test_wait_succeeds_with_exit_0_and_polls_with_the_injected_sleep(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    slept: list[float] = []
    end = ending(fake, "SUCCESS", task("a", "SUCCESS"))

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        end(seconds)

    r = invoke(s, "run", "--pipeline", "main", "--ensure", "--wait", sleep=sleep)
    assert r.exit_code == 0, r.stderr
    assert slept == [15] and "a: TERMINATED/SUCCESS" in r.stderr


def test_wait_failure_exits_1_and_prints_the_failure_block(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    results = {
        "results": [{"unit_id": "c", "status": "failed", "error": {"code": "X1", "message": "bad"}}]
    }
    fake.volume_files[f"{s.artifacts.as_posix()}/runs/nightly/run_results.json"] = json.dumps(
        results
    ).encode()
    r = invoke(
        s,
        "run",
        "--pipeline",
        "main",
        "--ensure",
        "--run-id",
        "nightly",
        sleep=ending(fake, "FAILED", task("c", "FAILED")),
    )
    assert r.exit_code == 1
    assert "ended: TERMINATED/FAILED" in r.stderr and "c: X1 bad" in r.stderr


def test_wait_api_error_is_pld009(sim: Callable[..., Sim], fake: FakeDatabricks) -> None:
    s = sim()

    def sleep(seconds: float) -> None:
        raise AssertionError("not reached")

    from pipeloom_databricks.client import ApiError

    fake.fail["get_run"] = ApiError(500, "down", "INTERNAL_ERROR")
    r = invoke(s, "run", "--pipeline", "main", "--ensure", sleep=sleep)
    assert r.exit_code == 1 and "PLD009" in r.stderr


# -- repair --------------------------------------------------------------------------------------


def failed_run(fake: FakeDatabricks, params: dict[str, str] | None = None) -> int:
    job_id = fake.add_job(
        {"name": "pipeloom_sim_main_prod", "tags": {"pipeloom_job": "sim.main.prod"}}
    )
    fake.set_run(
        700,
        job_id=job_id,
        result_state="FAILED",
        job_parameters=params or {},
        tasks=[task("c", "FAILED"), task(FINALIZE_KEY, "FAILED"), task("a", "SUCCESS")],
    )
    return job_id


def test_repair_by_job_run_id_prints_the_repair_and_no_wait_stops(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    failed_run(fake)
    r = invoke(s, "repair", "--pipeline", "main", "--job-run-id", "700", "--no-wait")
    assert r.exit_code == 0, r.stderr
    doc = first_line(r)
    assert doc["job_run_id"] == 700 and doc["repair_id"] == fake.runs[700].latest_repair_id
    assert fake.repairs[0][1] == ("c", FINALIZE_KEY)


def test_repair_by_run_id_finds_the_job_and_waits(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    failed_run(fake, {"pipeloom_run_id": "nightly"})
    r = invoke(
        s,
        "repair",
        "--pipeline",
        "main",
        "--run-id",
        "nightly",
        "--wait",
        sleep=ending(fake, "SUCCESS"),
    )
    assert r.exit_code == 0, r.stderr
    assert fake.repairs[0][0] == 700


def test_repair_by_run_id_unknown_run_or_job_is_pld007(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    r = invoke(s, "repair", "--pipeline", "main", "--run-id", "x", "--no-wait")
    assert r.exit_code == 1 and "PLD007" in r.stderr and "no job" in r.stderr
    failed_run(fake)
    r = invoke(s, "repair", "--pipeline", "main", "--run-id", "x", "--no-wait")
    assert r.exit_code == 1 and "PLD007" in r.stderr and "no run 'x'" in r.stderr


def test_repair_needs_exactly_one_run_option(sim: Callable[..., Sim], fake: FakeDatabricks) -> None:
    s = sim()
    for extra in ((), ("--run-id", "a", "--job-run-id", "1")):
        r = invoke(s, "repair", "--pipeline", "main", *extra)
        assert r.exit_code == 2 and "exactly one" in r.stderr
    assert invoke(s, "repair", "--pipeline", "main", "--job-run-id", "0").exit_code == 2
    assert fake.calls == []


def test_repair_active_run_is_pld014_and_nothing_to_repair_exits_0(
    sim: Callable[..., Sim], fake: FakeDatabricks
) -> None:
    s = sim()
    fake.set_run(800, job_id=1, life_cycle_state="RUNNING")
    r = invoke(s, "repair", "--pipeline", "main", "--job-run-id", "800")
    assert r.exit_code == 1 and "PLD014" in r.stderr
    fake.set_run(801, job_id=1, result_state="SUCCESS", tasks=[task("a", "SUCCESS")])
    r = invoke(s, "repair", "--pipeline", "main", "--job-run-id", "801")
    assert r.exit_code == 0 and "nothing to repair" in r.stderr and r.stdout == ""


def test_pipeline_defaults_to_the_only_one(sim: Callable[..., Sim], fake: FakeDatabricks) -> None:
    s = sim()
    path = s.root / "pipeloom.yml"
    path.write_text(
        path.read_text(encoding="utf-8").replace(', extra: {select: ["e"]}', ""), encoding="utf-8"
    )
    fake.set_run(801, job_id=1, result_state="SUCCESS", tasks=[task("a", "SUCCESS")])
    r = invoke(s, "repair", "--job-run-id", "801")
    assert r.exit_code == 0, r.stderr
