"""Run a deployed job, run once without a job, find a run and wait, all on `FakeDatabricks`."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from typing import Any

import pytest

from pipeloom.diagnostics import Report, Severity
from pipeloom.graph.selectors import Selection
from pipeloom_databricks.bundle import Runtime
from pipeloom_databricks.client import ApiError, TaskRunInfo
from pipeloom_databricks.deploy import deploy
from pipeloom_databricks.keys import FINALIZE_KEY
from pipeloom_databricks.run import (
    SelectionError,
    effective_run_id,
    failure_lines,
    find_run,
    run_job,
    submit_ephemeral,
    wait_for_run,
)
from pipeloom_databricks.testing.fake import FakeDatabricks
from pipeloom_databricks.upload import MANIFEST_PATH

Sim = Any
VERSIONS = {"pipeloom": "1.2.3", "pipeloom-databricks": "1.2.3"}


def runtime(s: Sim) -> Runtime:
    return Runtime(artifacts_root=s.artifacts.as_posix(), state_shared=True)


def go(
    s: Sim, fake: FakeDatabricks, *, plan: Any = None, pipeline: str = "main", **kw: Any
) -> tuple[Any, Report]:
    report = Report()
    args: dict[str, Any] = {
        "selection": None,
        "run_id": None,
        "full_refresh": False,
        "ensure": False,
    }
    args.update(kw)
    started = run_job(
        fake,
        s.root,
        plan or s.plan,
        pipeline,
        runtime(s),
        report=report,
        versions=VERSIONS,
        **args,
    )
    return started, report


def deployed(s: Sim, fake: FakeDatabricks, plan: Any = None, pipeline: str = "main") -> int:
    result = deploy(
        fake, s.root, plan or s.plan, pipeline, runtime(s), report=Report(), versions=VERSIONS
    )
    assert result is not None
    return result.job_id


def codes(report: Report) -> list[str]:
    return [d.code for d in report.sorted()]


def test_a_missing_job_is_pld007_unless_ensure_deploys_it(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    started, report = go(s, fake)
    assert started is None and codes(report) == ["PLD007"]
    assert "sim.main.prod" in report.sorted()[0].message
    assert fake.called("run_now") == [] and fake.called("create_job") == []

    started, report = go(s, fake, ensure=True)
    assert started is not None and not report.sorted()
    assert len(fake.jobs) == 1 and started.job_id in fake.jobs
    assert fake.called("run_now") == [str(started.job_id)]


def test_a_deployed_job_runs_without_deploying_again(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    job_id = deployed(s, fake)
    before = len(fake.called("upload"))
    started, report = go(s, fake)
    assert started is not None and not report.sorted()
    assert (started.job_id, started.run_id) == (job_id, None)
    assert len(fake.called("upload")) == before and fake.called("reset_job") == []
    ((jid, params, only, token),) = fake.starts
    assert (jid, params, only, token) == (job_id, {"full_refresh": "false"}, None, None)
    assert fake.runs[started.job_run_id].job_id == job_id


def test_full_refresh_and_the_run_id_with_its_idempotency_token(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    job_id = deployed(s, fake)
    first, _ = go(s, fake, run_id="nightly-1", full_refresh=True)
    assert first is not None and first.run_id == "nightly-1"
    token = hashlib.sha256(f"{job_id}:nightly-1".encode()).hexdigest()
    assert fake.starts[0][1] == {"full_refresh": "true", "pipeloom_run_id": "nightly-1"}
    assert fake.starts[0][3] == token
    again, _ = go(s, fake, run_id="nightly-1", full_refresh=True)
    assert again is not None and again.job_run_id == first.job_run_id  # the token dedupes
    other, _ = go(s, fake, run_id="nightly-2")
    assert other is not None and other.job_run_id != first.job_run_id


def test_select_and_exclude_become_only_plus_finalize(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    deployed(s, fake)
    started, report = go(s, fake, selection=Selection(("c",)))
    assert started is not None and not report.sorted()
    assert fake.starts[-1][2] == ("c", FINALIZE_KEY)
    go(s, fake, selection=Selection(("a", "e"), ("e",)))
    assert fake.starts[-1][2] == ("a", FINALIZE_KEY)
    go(s, fake, selection=Selection())  # nothing chosen means everything
    assert fake.starts[-1][2] is None


def test_an_empty_or_unknown_selection_raises_and_starts_nothing(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    deployed(s, fake, pipeline="extra")
    with pytest.raises(SelectionError, match="no unit"):
        go(s, fake, pipeline="extra", selection=Selection(("a",)))  # a is not in pipeline extra
    with pytest.raises(SelectionError):
        go(s, fake, pipeline="extra", selection=Selection(("nope_not_a_model",)))
    assert fake.starts == []


def test_a_partly_selected_group_runs_whole_with_a_warning(sim: Callable[..., Sim]) -> None:
    s, fake = sim("by_tag"), FakeDatabricks()
    deployed(s, fake)
    started, report = go(s, fake, selection=Selection(("a",)))
    assert started is not None
    assert fake.starts[-1][2] == ("tag_grp", FINALIZE_KEY)
    (warning,) = report.sorted()
    assert warning.code == "PLD004" and warning.severity is Severity.WARNING
    assert "tag_grp" in warning.message
    _, whole = go(s, fake, selection=Selection(("a", "b")))
    assert whole.sorted() == ()


def test_a_changed_project_is_pld008_unless_ensure_deploys_it_first(
    sim: Callable[..., Sim], compile_project: Callable[..., Any]
) -> None:
    s, fake = sim(), FakeDatabricks()
    job_id = deployed(s, fake)
    (s.root / "models" / "m.yml").write_text(
        "models:\n  - {name: a, sql: select 2 as x}\n  - {name: e, sql: select 3 as x}\n",
        encoding="utf-8",
    )
    changed = compile_project(s.root, "prod")
    started, report = go(s, fake, plan=changed)
    assert started is None and codes(report) == ["PLD008"]
    assert str(job_id) in report.sorted()[0].message and fake.starts == []

    started, report = go(s, fake, plan=changed, ensure=True)
    assert started is not None and started.job_id == job_id and not report.sorted()
    assert fake.called("reset_job") == [str(job_id)]
    assert len(fake.starts) == 1


def test_more_than_one_tagged_job_is_pld006_and_api_errors_are_pld009(
    sim: Callable[..., Sim],
) -> None:
    s, fake = sim(), FakeDatabricks()
    for i in range(2):
        fake.add_job({"name": f"j{i}", "tags": {"pipeloom_job": "sim.main.prod"}})
    _, report = go(s, fake)
    assert codes(report) == ["PLD006"]

    s2, fake2 = sim(), FakeDatabricks()
    deployed(s2, fake2)
    fake2.fail["run_now"] = ApiError(503, "busy", "TEMPORARILY_UNAVAILABLE")
    started, report = go(s2, fake2)
    assert started is None and codes(report) == ["PLD009"]
    assert "HTTP 503" in report.sorted()[0].message


def test_the_page_url_is_best_effort(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    deployed(s, fake)
    started, _ = go(s, fake)
    assert started is not None and started.run_page_url is None
    fake.set_run(started.job_run_id, job_id=started.job_id, run_page_url="https://x/run/1")
    fake.fail["get_run"] = ApiError(500, "down")
    again, report = go(s, fake, run_id="r2")
    assert again is not None and again.run_page_url is None and not report.sorted()
    del fake.fail["get_run"]
    assert fake.get_run(started.job_run_id).run_page_url == "https://x/run/1"


# -- ephemeral -----------------------------------------------------------------------------------


def submit(s: Sim, fake: FakeDatabricks, **kw: Any) -> tuple[Any, Report]:
    report = Report()
    args: dict[str, Any] = {"selection": None, "full_refresh": False, "start_ms": 1_700_000_000_000}
    args.update(kw)
    started = submit_ephemeral(
        fake, s.root, s.plan, "main", runtime(s), report=report, versions=VERSIONS, **args
    )
    return started, report


def test_ephemeral_uploads_the_project_and_submits_with_literal_arguments(
    sim: Callable[..., Sim],
) -> None:
    s, fake = sim(), FakeDatabricks()
    started, report = submit(s, fake, run_id="once-1", full_refresh=True)
    assert started is not None and not report.sorted()
    assert started.job_id is None and started.run_id == "once-1"
    assert fake.called("create_job") == [] and fake.jobs == {}
    ((spec, token),) = fake.submissions
    assert token == hashlib.sha256(b"once-1").hexdigest()
    assert spec["run_name"] == "pipeloom_sim_main_prod"
    assert "job_clusters" not in spec
    tasks = {t["task_key"]: t for t in spec["tasks"]}  # type: ignore[index, union-attr]
    params = tasks["a"]["python_wheel_task"]["parameters"]
    assert "--run-id=once-1" in params and "--full-refresh=true" in params
    assert "--start-ms=1700000000000" in params
    assert not any(p.startswith("--job-") for p in params)
    root = next(p for p in params if p.startswith("--project-dir=")).split("=", 1)[1]
    assert f"{root}/{MANIFEST_PATH}" in fake.files and f"{root}/_pipeloom/complete" in fake.files


def test_ephemeral_generates_a_valid_run_id_and_repeats_are_deduplicated(
    sim: Callable[..., Sim],
) -> None:
    s, fake = sim(), FakeDatabricks()
    first, _ = submit(s, fake)
    assert first is not None and first.run_id is not None
    assert first.run_id.startswith("eph-") and len(first.run_id) <= 64
    same, _ = submit(s, fake, run_id=first.run_id)
    assert same is not None and same.job_run_id == first.job_run_id


def test_ephemeral_selection_keeps_the_chosen_tasks_and_a_finalize_that_waits_for_them(
    sim: Callable[..., Sim],
) -> None:
    s, fake = sim(), FakeDatabricks()
    started, _ = submit(s, fake, run_id="r", selection=Selection(("a+",)))
    assert started is not None
    spec = fake.submissions[0][0]
    tasks = {t["task_key"]: t for t in spec["tasks"]}  # type: ignore[index, union-attr]
    assert sorted(tasks) == ["a", "b", FINALIZE_KEY]
    assert tasks["b"]["depends_on"] == [{"task_key": "a"}]
    assert tasks[FINALIZE_KEY]["depends_on"] == [{"task_key": "b"}]

    only_c, _ = submit(s, fake, run_id="r2", selection=Selection(("c",)))
    assert only_c is not None
    tasks = {t["task_key"]: t for t in fake.submissions[1][0]["tasks"]}  # type: ignore[index, union-attr]
    assert sorted(tasks) == ["c", FINALIZE_KEY]
    assert "depends_on" not in tasks["c"]
    assert tasks[FINALIZE_KEY]["depends_on"] == [{"task_key": "c"}]
    with pytest.raises(SelectionError):
        submit(s, fake, selection=Selection(("nope_not_a_model",)))


def test_ephemeral_on_a_job_cluster_is_pld004_and_uploads_nothing(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    block = {
        **s.plan.metadata.deploy,
        "compute": {"default": {"kind": "job_cluster", "new_cluster": {"num_workers": 1}}},
    }
    plan = s.plan.model_copy(
        update={"metadata": s.plan.metadata.model_copy(update={"deploy": block})}
    )
    report = Report()
    started = submit_ephemeral(
        fake, s.root, plan, "main", runtime(s), selection=None, full_refresh=False, report=report
    )
    assert started is None and "PLD004" in codes(report)
    assert fake.submissions == [] and not any(p.endswith("complete") for p in fake.files)


def test_ephemeral_api_errors_are_pld009(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    fake.fail["submit_run"] = ApiError(400, "bad spec", "INVALID_PARAMETER_VALUE")
    started, report = submit(s, fake)
    assert started is None and codes(report) == ["PLD009"]


# -- find_run, waiting and the failure block -----------------------------------------------------


def test_find_run_by_dbx_id_by_parameter_and_not_at_all() -> None:
    fake = FakeDatabricks()
    fake.set_run(500, job_id=7, job_parameters={"pipeloom_run_id": "nightly"})
    fake.set_run(501, job_id=7, job_parameters={"pipeloom_run_id": ""})
    fake.set_run(502, job_id=8, job_parameters={"pipeloom_run_id": "nightly"})
    by_param = find_run(fake, 7, "nightly")
    assert by_param is not None and by_param.run_id == 500
    assert fake.called("list_runs") == ["7"]
    by_id = find_run(fake, 7, "dbx-501")
    assert by_id is not None and by_id.run_id == 501
    assert find_run(fake, 7, "dbx-502") is None  # a run of another job
    assert find_run(fake, 7, "dbx-9999") is None  # 404
    assert find_run(fake, 7, "unknown") is None
    fake.fail["get_run"] = ApiError(500, "down")
    with pytest.raises(ApiError):
        find_run(fake, 7, "dbx-501")


def test_effective_run_id() -> None:
    fake = FakeDatabricks()
    named = fake.set_run(1, job_parameters={"pipeloom_run_id": "x"})
    empty = fake.set_run(2, job_parameters={"pipeloom_run_id": ""})
    assert (effective_run_id(named), effective_run_id(empty)) == ("x", "dbx-2")
    assert effective_run_id(fake.set_run(3)) == "dbx-3"


def _task(key: str, life: str, result: str | None) -> TaskRunInfo:
    return TaskRunInfo(key, 1, 1, life, result, None, None)


def test_wait_polls_until_the_run_ends_and_says_each_transition() -> None:
    fake = FakeDatabricks()
    fake.set_run(5, job_id=1, life_cycle_state="PENDING")
    steps = iter(
        [
            ("RUNNING", None, [_task("a", "RUNNING", None)]),
            ("RUNNING", None, [_task("a", "TERMINATED", "SUCCESS")]),
            ("TERMINATED", "SUCCESS", [_task("a", "TERMINATED", "SUCCESS")]),
        ]
    )
    slept: list[float] = []

    def sleep(seconds: float) -> None:
        slept.append(seconds)
        life, result, tasks = next(steps)
        fake.set_run(5, job_id=1, life_cycle_state=life, result_state=result, tasks=tasks)

    said: list[str] = []
    run = wait_for_run(fake, 5, sleep=sleep, say=said.append)
    assert run.result_state == "SUCCESS" and slept == [15, 15, 15]
    assert said == [
        "run: PENDING",
        "run: RUNNING",
        "a: RUNNING",
        "a: TERMINATED/SUCCESS",
        "run: TERMINATED/SUCCESS",
    ]


def test_wait_after_a_repair_ignores_the_state_from_before_it() -> None:
    fake = FakeDatabricks()
    fake.set_run(5, job_id=1, result_state="FAILED", latest_repair_id=10)
    polls: list[int] = []

    def sleep(seconds: float) -> None:
        polls.append(1)
        fake.set_run(5, job_id=1, result_state="SUCCESS", latest_repair_id=11)

    run = wait_for_run(fake, 5, sleep=sleep, say=lambda _: None, repair_id=11)
    assert run.result_state == "SUCCESS" and polls == [1]


def test_wait_lets_api_errors_through() -> None:
    fake = FakeDatabricks()
    with pytest.raises(ApiError):
        wait_for_run(fake, 404, sleep=lambda _: None, say=lambda _: None)


def test_failure_lines_read_run_results_from_the_volume() -> None:
    fake = FakeDatabricks()
    root = "/Volumes/main/ops/pipeloom"
    doc = {
        "results": [
            {"unit_id": "a", "status": "succeeded", "error": None},
            {"unit_id": "c", "status": "failed", "error": {"code": "PLX106", "message": "boom"}},
            {"unit_id": "t", "status": "timed_out", "error": None},
        ]
    }
    fake.volume_files[f"{root}/runs/r1/run_results.json"] = json.dumps(doc).encode("utf-8")
    assert failure_lines(fake, root + "/", "r1") == ["c: PLX106 boom", "t:"]
    assert failure_lines(fake, None, "r1") == []
    missing = failure_lines(fake, root, "nope")
    assert len(missing) == 1 and "run_results.json not read" in missing[0]
    fake.volume_files[f"{root}/runs/bad/run_results.json"] = b"{not json"
    assert "not usable" in failure_lines(fake, root, "bad")[0]
    fake.volume_files[f"{root}/runs/odd/run_results.json"] = b"[1]"
    assert "not usable" in failure_lines(fake, root, "odd")[0]
