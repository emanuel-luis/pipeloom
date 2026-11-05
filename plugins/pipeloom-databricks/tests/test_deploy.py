"""Direct deploy on `FakeDatabricks`: created, unchanged, updated, marker, prune and the errors."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from pipeloom.diagnostics import Report, Severity
from pipeloom.secrets.redaction import Redactor
from pipeloom_databricks import upload as upload_module
from pipeloom_databricks.bundle import Runtime
from pipeloom_databricks.client import ApiError
from pipeloom_databricks.deploy import DuplicateJobs, deploy, find_job
from pipeloom_databricks.testing.fake import FakeDatabricks
from pipeloom_databricks.upload import MANIFEST_PATH, MARKER_PATH, collect

Sim = Any
PARENT = "/Workspace/Users/me@example.com/.pipeloom/sim/main/prod"
VERSIONS = {"pipeloom": "1.2.3", "pipeloom-databricks": "1.2.3"}


def run(
    s: Sim,
    fake: FakeDatabricks,
    *,
    plan: Any = None,
    redactor: Redactor | None = None,
) -> tuple[Any, Report]:
    report = Report()
    runtime = Runtime(artifacts_root=s.artifacts.as_posix(), state_shared=True)
    result = deploy(
        fake,
        s.root,
        plan or s.plan,
        "main",
        runtime,
        report=report,
        redactor=redactor,
        versions=VERSIONS,
    )
    return result, report


def codes(report: Report) -> list[str]:
    return [d.code for d in report.sorted()]


def hash_folders(fake: FakeDatabricks) -> list[str]:
    return sorted(e.path.rsplit("/", 1)[-1] for e in fake.list_dir(PARENT) if e.is_dir)


def test_first_deploy_creates_the_job_and_uploads_the_project(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    result, report = run(s, fake)
    assert result is not None and not report.sorted()
    assert result.action == "created"
    assert result.root.startswith(PARENT + "/") and len(result.root.rsplit("/", 1)[1]) == 12

    settings = fake.jobs[result.job_id]
    tags = settings["tags"]
    assert isinstance(tags, dict) and tags["pipeloom_job"] == "sim.main.prod"
    assert settings["name"] == "pipeloom_sim_main_prod"
    first = next(t for t in settings["tasks"] if t["task_key"] == "a")  # type: ignore[index, union-attr]
    params = first["python_wheel_task"]["parameters"]
    assert f"--project-dir={result.root}" in params
    assert f"--manifest={result.root}/{MANIFEST_PATH}" in params

    uploaded = sorted(p[len(result.root) + 1 :] for p in fake.files if p.startswith(result.root))
    assert uploaded == sorted(["pipeloom.yml", "models/m.yml", MANIFEST_PATH, MARKER_PATH])
    order = [a for m, a in fake.calls if m == "upload"]
    assert order[-1] == f"{result.root}/{MARKER_PATH}"
    assert fake.files[f"{result.root}/models/m.yml"] == (s.root / "models" / "m.yml").read_bytes()
    assert fake.called("current_user") == [""]


def test_a_second_deploy_of_the_same_project_changes_nothing(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    first, _ = run(s, fake)
    before = list(fake.calls)
    second, report = run(s, fake)
    assert second is not None and second.action == "unchanged"
    assert (second.job_id, second.root) == (first.job_id, first.root)
    new = fake.calls[len(before) :]
    assert {m for m, _ in new} <= {"current_user", "list_jobs"}
    assert not report.sorted()


def test_a_changed_project_resets_the_job_to_a_new_folder(
    sim: Callable[..., Sim], compile_project: Callable[..., Any]
) -> None:
    s, fake = sim(), FakeDatabricks()
    first, _ = run(s, fake)
    (s.root / "models" / "m.yml").write_text(
        "models:\n  - {name: a, sql: select 2 as x}\n  - {name: e, sql: select 3 as x}\n",
        encoding="utf-8",
    )
    second, _ = run(s, fake, plan=compile_project(s.root, "prod"))
    assert second is not None and second.action == "updated"
    assert second.job_id == first.job_id and second.root != first.root
    assert fake.called("reset_job") == [str(first.job_id)]
    assert len(fake.jobs) == 1
    assert any(p.startswith(first.root) for p in fake.files)  # the old copy stays for now


def test_a_finished_upload_is_not_repeated(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    first, _ = run(s, fake)
    fake.jobs.clear()
    uploads = len(fake.called("upload"))
    again, _ = run(s, fake)
    assert again is not None and again.action == "created" and again.root == first.root
    assert len(fake.called("upload")) == uploads


def test_an_unfinished_upload_is_redone(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    first, _ = run(s, fake)
    del fake.files[f"{first.root}/{MARKER_PATH}"]
    fake.jobs.clear()
    uploads = len(fake.called("upload"))
    run(s, fake)
    assert len(fake.called("upload")) > uploads
    assert f"{first.root}/{MARKER_PATH}" in fake.files


def test_prune_keeps_the_current_and_the_newest_others(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    roots = []
    for i in range(5):
        (s.root / "note.txt").write_text(f"v{i}", encoding="utf-8")
        result, _ = run(s, fake)
        roots.append(result.root.rsplit("/", 1)[1])
    left = hash_folders(fake)
    assert len(left) == 3
    assert left == sorted(roots[2:])
    deleted = fake.called("delete")
    assert len(deleted) == 2 and all(d.startswith(PARENT) for d in deleted)


def test_prune_failures_are_warnings(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    for i in range(4):
        (s.root / "note.txt").write_text(f"v{i}", encoding="utf-8")
        if i == 3:
            fake.fail["delete"] = ApiError(403, "no delete for you")
        result, report = run(s, fake)
    assert result is not None and not report.has_errors
    (warning,) = report.sorted()
    assert (warning.code, warning.severity) == ("PLD009", Severity.WARNING)
    assert "no delete for you" in warning.message


def test_workspace_root_option_skips_the_user_lookup(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    plan = s.plan.model_copy(
        update={
            "metadata": s.plan.metadata.model_copy(
                update={"deploy": {**s.plan.metadata.deploy, "workspace_root": "/Shared/pl/"}}
            )
        }
    )
    result, _ = run(s, fake, plan=plan)
    assert result is not None and result.root.startswith("/Shared/pl/sim/main/prod/")
    assert fake.called("current_user") == []


def test_more_than_one_tagged_job_is_pld006(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    ids = sorted(
        fake.add_job({"name": f"j{i}", "tags": {"pipeloom_job": "sim.main.prod"}}) for i in range(2)
    )
    result, report = run(s, fake)
    assert result is None and codes(report) == ["PLD006"]
    assert str(ids) in report.sorted()[0].message
    assert fake.called("create_job") == [] and fake.called("reset_job") == []


def test_find_job_by_name_then_by_scan() -> None:
    fake = FakeDatabricks()
    fake.add_job({"name": "other", "tags": {"x": "y"}})
    renamed = fake.add_job({"name": "renamed by hand", "tags": {"pipeloom_job": "p.m.t"}})
    found = find_job(fake, "p.m.t", name="pipeloom_p_m_t")
    assert found is not None and found.job_id == renamed
    assert fake.called("list_jobs") == ["pipeloom_p_m_t", ""]
    assert find_job(fake, "nope.nope.nope") is None
    fake.add_job({"name": "twin", "tags": {"pipeloom_job": "p.m.t"}})
    with pytest.raises(DuplicateJobs) as err:
        find_job(fake, "p.m.t")
    assert len(err.value.job_ids) == 2


def test_api_failures_are_pld009_and_masked(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    fake.fail["create_job"] = ApiError(500, "backend said hunter2-secret", "INTERNAL_ERROR")
    result, report = run(s, fake, redactor=Redactor(("hunter2-secret",)))
    assert result is None and codes(report) == ["PLD009"]
    message = report.sorted()[0].message
    assert "hunter2-secret" not in message and "***" in message
    assert "HTTP 500, INTERNAL_ERROR" in message


def test_a_failing_user_lookup_is_pld009(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    fake.fail["current_user"] = ApiError(401, "bad token")
    result, report = run(s, fake)
    assert result is None and codes(report) == ["PLD009"]
    assert fake.files == {}


def test_render_errors_stop_before_any_upload(sim: Callable[..., Sim]) -> None:
    s, fake = sim(), FakeDatabricks()
    report = Report()
    runtime = Runtime(artifacts_root=None, state_shared=True)
    assert deploy(fake, s.root, s.plan, "main", runtime, report=report) is None
    assert "PLD004" in codes(report)
    assert fake.files == {} and fake.called("create_job") == []


def test_collect_skips_caches_dot_entries_and_the_artifacts_root(sim: Callable[..., Sim]) -> None:
    s = sim()
    for rel in ("__pycache__/x.pyc", ".hidden/a.txt", ".env", "out/run.json", "mod.py", "mod.pyc"):
        path = s.root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")
    payload = collect(s.root, s.plan, (s.root / "out").as_posix(), Report())
    assert payload is not None
    assert [p for p, _ in payload.files] == [
        MANIFEST_PATH,
        "mod.py",
        "models/m.yml",
        "pipeloom.yml",
    ]
    again = collect(s.root, s.plan, (s.root / "out").as_posix(), Report())
    assert again is not None and again.digest == payload.digest
    (s.root / "mod.py").write_text("y", encoding="utf-8")
    changed = collect(s.root, s.plan, (s.root / "out").as_posix(), Report())
    assert changed is not None and changed.digest != payload.digest


def test_unuploadable_files_are_pld012(
    sim: Callable[..., Sim], monkeypatch: pytest.MonkeyPatch
) -> None:
    s, fake = sim(), FakeDatabricks()
    (s.root / "nb.py").write_text("# Databricks notebook source\nprint(1)\n", encoding="utf-8")
    (s.root / "q.sql").write_text("-- Databricks notebook source\nselect 1\n", encoding="utf-8")
    (s.root / "x.ipynb").write_text("{}", encoding="utf-8")
    (s.root / "big.bin").write_bytes(b"0" * 6000)
    monkeypatch.setattr(upload_module, "MAX_FILE_BYTES", 5000)
    result, report = run(s, fake)
    assert result is None
    found = report.sorted()
    assert {d.code for d in found} == {"PLD012"}
    files = sorted(d.location.file for d in found if d.location and d.location.file)
    assert files == ["big.bin", "nb.py", "q.sql", "x.ipynb"]
    assert fake.calls == []


def test_python_paths_outside_the_project_are_pld012(sim: Callable[..., Sim]) -> None:
    s = sim()
    meta = s.plan.metadata.model_copy(update={"python_paths": ("..",)})
    plan = s.plan.model_copy(update={"metadata": meta})
    (s.root.parent / "elsewhere").mkdir(exist_ok=True)
    report = Report()
    assert collect(s.root, plan, None, report) is None
    assert codes(report) == ["PLD012"]
    assert Path(s.root).exists()
