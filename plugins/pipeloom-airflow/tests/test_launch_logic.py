"""start_or_attach and load_project on FakeDatabricks: no Airflow, no workspace."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from pipeloom.diagnostics import Report
from pipeloom.graph.selectors import Selection
from pipeloom.plan.model import Plan
from pipeloom_airflow.databricks.launch_logic import LaunchResult, load_project, start_or_attach
from pipeloom_databricks.api import FINALIZE_KEY, ApiError, Runtime, TaskRunInfo, deploy
from pipeloom_databricks.testing.fake import FakeDatabricks

VERSIONS = {"pipeloom": "1.2.3", "pipeloom-databricks": "1.2.3"}
RUNTIME = Runtime(artifacts_root="/Volumes/main/ops/pipeloom", state_shared=True)
RID = "af-0123456789abcdef"
PROJECT = """config_version: 1
name: p
engine: {kind: memory}
catalog: {kind: memory}
state: {kind: json, path: state}
artifacts: {root: /Volumes/main/ops/pipeloom}
deploy: {kind: databricks}
targets:
  prod: {}
"""
MODELS = """models:
  - {name: a, sql: select 1 as x}
  - {name: b, sql: select * from a, inputs: {a: {ref: a}}}
"""


@pytest.fixture
def project(tmp_path: Path) -> tuple[Path, Plan]:
    (tmp_path / "models").mkdir()
    (tmp_path / "pipeloom.yml").write_text(PROJECT, encoding="utf-8")
    (tmp_path / "models" / "m.yml").write_text(MODELS, encoding="utf-8")
    report = Report()
    loaded = load_project(
        tmp_path.as_posix(), "prod", {}, expect_plan_hash="", env={}, report=report
    )
    assert loaded is None and [d.code for d in report.sorted()] == ["PLX103"]
    from pipeloom.api import Project, ProjectOptions

    plan = Project(ProjectOptions(project_dir=tmp_path, target="prod", env={})).compile().value
    assert plan is not None
    return tmp_path, plan


def go(
    project: tuple[Path, Plan], fake: FakeDatabricks, **kw: Any
) -> tuple[LaunchResult | None, list[str]]:
    root, plan = project
    report = Report()
    args: dict[str, Any] = {
        "run_id": RID,
        "full_refresh": False,
        "selection": Selection(),
        "ensure_job": False,
    }
    args.update(kw)
    result = start_or_attach(
        fake, root, plan, "main", RUNTIME, report=report, versions=VERSIONS, **args
    )
    return result, [d.code for d in report.sorted()]


def deployed(project: tuple[Path, Plan], fake: FakeDatabricks) -> int:
    result = deploy(
        fake, project[0], project[1], "main", RUNTIME, report=Report(), versions=VERSIONS
    )
    assert result is not None
    return result.job_id


def _task(key: str, result: str, attempt: int = 0) -> TaskRunInfo:
    return TaskRunInfo(key, 1, attempt, "TERMINATED", result, None, None)


def test_load_project_binds_the_runtime(project: tuple[Path, Plan]) -> None:
    root, plan = project
    report = Report()
    loaded = load_project(
        root.as_posix(),
        "prod",
        {},
        expect_plan_hash=plan.metadata.plan_hash.upper(),
        env={},
        report=report,
    )
    assert loaded is not None, report.sorted()
    assert loaded[0].metadata.plan_hash == plan.metadata.plan_hash
    assert loaded[1] == Runtime("/Volumes/main/ops/pipeloom", True, 30)


def test_no_job_is_pld007_unless_ensure_deploys_it(project: tuple[Path, Plan]) -> None:
    fake = FakeDatabricks()
    result, codes = go(project, fake)
    assert result is None and codes == ["PLD007"] and fake.starts == []
    result, codes = go(project, fake, ensure_job=True, full_refresh=True)
    assert result is not None and codes == []
    assert (result.action, result.run_id, result.repair_id) == ("started", RID, None)
    ((job_id, params, only, _),) = fake.starts
    assert job_id == result.job_id and only is None
    assert params == {"full_refresh": "true", "pipeloom_run_id": RID}


def test_a_stale_job_is_pld008(project: tuple[Path, Plan]) -> None:
    fake = FakeDatabricks()
    job_id = deployed(project, fake)
    settings = fake.jobs[job_id]
    assert isinstance(settings["tags"], dict)
    settings["tags"]["pipeloom_settings_hash"] = "0" * 64
    result, codes = go(project, fake)
    assert result is None and codes == ["PLD008"]


def test_a_retried_launch_attaches_to_the_running_run(project: tuple[Path, Plan]) -> None:
    fake = FakeDatabricks()
    deployed(project, fake)
    first, _ = go(project, fake)
    again, codes = go(project, fake)
    assert first is not None and again is not None and codes == []
    assert again.action == "attached" and again.job_run_id == first.job_run_id
    assert len(fake.starts) == 1


def test_a_selection_runs_only_its_tasks_and_finalize(project: tuple[Path, Plan]) -> None:
    fake = FakeDatabricks()
    deployed(project, fake)
    result, _ = go(project, fake, selection=Selection(("b",), ()))
    assert result is not None
    assert fake.starts[0][2] == ("b", FINALIZE_KEY)


def test_a_succeeded_run_is_attached_and_a_failed_one_repaired(
    project: tuple[Path, Plan],
) -> None:
    fake = FakeDatabricks()
    job_id = deployed(project, fake)
    first, _ = go(project, fake)
    assert first is not None
    params = {"pipeloom_run_id": RID}
    ok = [_task("a", "SUCCESS"), _task("b", "SUCCESS"), _task(FINALIZE_KEY, "SUCCESS")]
    fake.set_run(
        first.job_run_id, job_id=job_id, result_state="SUCCESS", tasks=ok, job_parameters=params
    )
    attached, _ = go(project, fake)
    assert attached is not None and attached.action == "attached" and fake.repairs == []

    bad = [_task("a", "SUCCESS"), _task("b", "FAILED", 1), _task(FINALIZE_KEY, "FAILED")]
    fake.set_run(
        first.job_run_id, job_id=job_id, result_state="FAILED", tasks=bad, job_parameters=params
    )
    repaired, codes = go(project, fake)
    assert repaired is not None and codes == []
    assert repaired.action == "repaired" and repaired.repair_id is not None
    assert dict(repaired.baseline) == {"b": 2, FINALIZE_KEY: 1}
    assert fake.repairs == [(first.job_run_id, ("b", FINALIZE_KEY), None)]
    assert repaired.document()["baseline"] == {"b": 2, FINALIZE_KEY: 1}


def test_api_errors_are_pld009(project: tuple[Path, Plan]) -> None:
    fake = FakeDatabricks()
    fake.fail["list_jobs"] = ApiError(503, "unavailable")
    result, codes = go(project, fake)
    assert result is None and codes == ["PLD009"]
