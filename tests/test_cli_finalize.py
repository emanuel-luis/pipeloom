"""`pipeloom finalize` and `--orchestrator-id`: every flag, precedence, exit codes."""

from __future__ import annotations

import errno
import json
import os
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

from pipeloom.artifacts.io import write_json_once
from pipeloom.cli import main
from pipeloom.events.model import Event
from tests.conftest import make_project

YML = """config_version: 1
name: p
targets:
  ci:
    engine: {kind: memory}
    catalog: {kind: memory}
"""
MODELS = """sources:
  - {name: raw, format: memory, location: raw/missing}
models:
  - {name: a, sql: select 1 as x}
  - {name: b, sql: select * from a, inputs: {a: {ref: a}}}
  - {name: bad, sql: select * from raw, inputs: {raw: {source: raw}}}
  - {name: after_bad, sql: select * from bad, inputs: {bad: {ref: bad}}}
  - {name: z, sql: select 2 as x}
"""
ENV = {
    "PIPELOOM_TARGET": "",
    "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "",
    "PIPELOOM_WORKERS": "",
    "PIPELOOM_LOG_FORMAT": "",
    "PIPELOOM_RUN_STARTED_AT": "",
}
UNITS = ["a", "after_bad", "b", "bad", "z"]


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": MODELS})


def invoke(command: str, root: Path, *args: str, env: dict[str, str] | None = None) -> Result:
    argv = [command, "--project-dir", str(root), "--target", "ci", "--log-format", "json", *args]
    return CliRunner().invoke(main, argv, env={**ENV, **(env or {})})


def finalize(root: Path, *args: str, env: dict[str, str] | None = None) -> Result:
    return invoke("finalize", root, "--run-id", "r1", *args, env=env)


def events(result: Result) -> list[Event]:
    return [
        Event.model_validate(json.loads(ln))
        for ln in result.stderr.splitlines()
        if ln.startswith("{")
    ]


def results(root: Path, run_id: str = "r1") -> dict[str, Any]:
    path = root / "target" / "runs" / run_id / "run_results.json"
    doc: dict[str, Any] = json.loads(path.read_bytes().decode("utf-8"))
    return doc


def rows(root: Path) -> dict[str, dict[str, Any]]:
    return {r["unit_id"]: r for r in results(root)["results"]}


def states(root: Path, units: dict[str, Any], schema: str = "pipeloom.unit_states/1") -> Path:
    path = root / "states.json"
    path.write_text(json.dumps({"schema": schema, "units": units}), encoding="utf-8")
    return path


def ran_a(root: Path) -> None:
    """Unit a ran and wrote its attempt file; nothing else did."""
    assert invoke("run", root, "-s", "a", "--run-id", "r1").exit_code == 0


# -- orchestrator ids on run ---------------------------------------------------------------


def test_run_writes_orchestrator_ids_everywhere(root: Path) -> None:
    r = invoke(
        "run",
        root,
        "-s",
        "a",
        "--run-id",
        "r1",
        "--orchestrator-id",
        "job_id=7",
        "--orchestrator-id",
        "job_run_id=42",
        "--orchestrator-id",
        "job_id=8",
    )
    assert r.exit_code == 0, r.stderr
    ids = {"job_id": "8", "job_run_id": "42"}
    attempt = json.loads(
        (root / "target/runs/r1/units/a/attempt-1.json").read_text(encoding="utf-8")
    )
    assert attempt["orchestrator_ids"] == ids
    doc = results(root)
    assert doc["run"]["identity"]["orchestrator_ids"] == ids
    assert doc["results"][0]["orchestrator_ids"] == ids
    seen = events(r)
    assert seen and all(
        e.context == {"orchestrator_id.job_id": "8", "orchestrator_id.job_run_id": "42"}
        for e in seen
    )


@pytest.mark.parametrize(
    "value",
    ["Bad=1", "1a=1", "a-b=1", "k=", "novalue", "k=\x07", "k=" + "x" * 257, "a" * 65 + "=1"],
)
@pytest.mark.parametrize("command", ["run", "finalize"])
def test_orchestrator_id_validation_exits_2(root: Path, command: str, value: str) -> None:
    extra = ["--run-id", "r1", "--orchestrator-id", value]
    assert invoke(command, root, *extra).exit_code == 2


def test_orchestrator_id_limits_are_inclusive(root: Path) -> None:
    ok = ["--orchestrator-id", f"{'k' * 64}={'v' * 256}", "--orchestrator-id", "a0_=x y"]
    assert invoke("run", root, "-s", "a", "--run-id", "r1", *ok).exit_code == 0


def test_process_children_get_the_ids(root: Path) -> None:
    from pipeloom.runtime.executors.process import ChildCommand

    cmd = ChildCommand(
        project_dir=root,
        target="ci",
        manifest=root / "m.json",
        run_id="r",
        plan_hash="0" * 64,
        full_refresh=False,
        vars=None,
        plugins=(),
        orchestrator_ids={"job_id": "7"},
    )
    argv = cmd.argv("u")
    assert argv[argv.index("--orchestrator-id") + 1] == "job_id=7"


# -- finalize ------------------------------------------------------------------------------


def test_every_other_unit_is_not_run_and_neutral(root: Path) -> None:
    ran_a(root)
    r = finalize(root)
    assert r.exit_code == 0, r.stderr
    got = rows(root)
    assert sorted(got) == UNITS
    assert got["a"]["status"] == "succeeded"
    for unit in ("b", "bad", "after_bad", "z"):
        assert (got[unit]["status"], got[unit]["skip_reason"]) == ("skipped", "not_run")
    assert results(root)["run"]["status"] == "succeeded"
    types = [e.type for e in events(r)]
    assert "run.completed" in types and "run.started" not in types
    assert next(e for e in events(r) if e.type == "invocation.started").data["command"] == (
        "finalize"
    )


def test_external_states_fill_the_missing_units(root: Path) -> None:
    ran_a(root)
    path = states(
        root,
        {
            "b": {
                "status": "failed",
                "code": "PLD011",
                "message": "task ended",
                "orchestrator_ids": {"task_run_id": "9"},
            },
            "bad": {"status": "cancelled"},
            "after_bad": {"status": "skipped", "reason": "upstream_failed"},
            "z": {"status": "excluded"},
        },
    )
    r = finalize(root, "--unit-states", str(path), "--orchestrator-id", "job_id=5")
    assert r.exit_code == 1, r.stderr  # a failed unit wins over a cancelled one
    got = rows(root)
    assert sorted(got) == ["a", "after_bad", "b", "bad"]  # z is excluded: no row
    assert got["b"]["status"] == "failed"
    assert got["b"]["error"]["code"] == "PLD011" and got["b"]["error"]["message"] == "task ended"
    assert got["b"]["orchestrator_ids"] == {"job_id": "5", "task_run_id": "9"}
    assert got["bad"]["status"] == "cancelled"
    assert (got["after_bad"]["status"], got["after_bad"]["skip_reason"]) == (
        "skipped",
        "upstream_failed",
    )
    assert results(root)["run"]["status"] == "failed"
    assert "PLD011" in r.stderr


def test_the_attempt_file_beats_the_external_state(root: Path) -> None:
    ran_a(root)
    path = states(root, {"a": {"status": "failed"}, "b": {"status": "excluded"}})
    r = finalize(root, "--unit-states", str(path))
    assert r.exit_code == 0, r.stderr
    got = rows(root)
    assert got["a"]["status"] == "succeeded"
    assert "b" not in got


def test_a_failed_attempt_file_fails_the_run(root: Path) -> None:
    invoke("run", root, "--run-id", "r1", "--workers", "1")
    r = finalize(root)
    assert r.exit_code == 1
    got = rows(root)
    assert got["bad"]["status"] == "failed"
    # the scheduler's skip wrote no file: the failed upstream tells why
    assert got["after_bad"]["skip_reason"] == "upstream_failed"
    assert got["z"]["status"] == "succeeded"


CHAIN = """models:
  - {name: a, sql: select 1 as x}
  - {name: b, sql: select * from a, inputs: {a: {ref: a}}}
  - {name: c, sql: select * from b, inputs: {b: {ref: b}}}
  - {name: d, sql: select 2 as x}
"""


def test_descendants_of_a_failed_unit_are_upstream_failed(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": CHAIN})
    path = states(root, {"a": {"status": "failed"}})
    r = finalize(root, "--unit-states", str(path))
    assert r.exit_code == 1, r.stderr
    got = rows(root)
    assert got["a"]["status"] == "failed"
    for unit in ("b", "c"):  # c is two steps away: skipped rows propagate
        assert (got[unit]["status"], got[unit]["skip_reason"]) == ("skipped", "upstream_failed")
    assert got["d"]["skip_reason"] == "not_run"


def test_an_excluded_or_unknown_upstream_does_not_fail_descendants(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": CHAIN})
    path = states(root, {"a": {"status": "excluded"}})
    assert finalize(root, "--unit-states", str(path)).exit_code == 0
    got = rows(root)
    assert "a" not in got and got["b"]["skip_reason"] == "not_run"


def _hold(root: Path, run_id: str) -> Any:
    from pipeloom.api import Project, ProjectOptions

    bound = Project(ProjectOptions(project_dir=root, target="ci", env={})).bind().value
    assert bound is not None
    store = bound.plugins["state_stores"].create(None)
    return store, store.lock("p/ci", run_id)


def _running(store: Any) -> None:
    from datetime import UTC, datetime

    from pipeloom.state.types import RunRecord

    store.record_run(
        RunRecord(
            run_id="r1",
            project="p",
            target="ci",
            plan_hash="0" * 64,
            status="running",
            started_at=datetime(2026, 11, 4, tzinfo=UTC),
            invocation_ids=("i",),
        )
    )


def test_finalize_refuses_while_the_same_run_is_live(root: Path) -> None:
    ran_a(root)
    store, lock = _hold(root, "r1")
    try:
        _running(store)
        with lock:
            r = finalize(root)
    finally:
        store.close()
    assert r.exit_code == 1 and "PLX105" in r.stderr and "still running" in r.stderr
    assert finalize(root).exit_code == 0  # released: nothing holds it any more


def test_a_lock_without_a_running_record_does_not_block_finalize(root: Path) -> None:
    """What a finalize that crashed leaves behind: its lock, and no `running` record."""
    ran_a(root)
    store, lock = _hold(root, "r1")
    try:
        with lock:
            r = finalize(root)
    finally:
        store.close()
    assert r.exit_code == 0, r.stderr


def test_finalize_ignores_a_lock_of_another_run(root: Path) -> None:
    ran_a(root)
    store, lock = _hold(root, "other")
    try:
        _running(store)
        with lock:
            r = finalize(root)
    finally:
        store.close()
    assert r.exit_code == 0, r.stderr


def test_cancelled_alone_exits_130(root: Path) -> None:
    ran_a(root)
    units = {u: {"status": "excluded"} for u in ("b", "after_bad", "z")}
    path = states(root, {**units, "bad": {"status": "cancelled"}})
    r = finalize(root, "--unit-states", str(path))
    assert r.exit_code == 130
    assert results(root)["run"]["status"] == "cancelled"


def test_skipped_for_another_reason_is_neutral(root: Path) -> None:
    ran_a(root)
    skipped = {"status": "skipped", "reason": "condition_false"}
    path = states(root, {u: skipped for u in ("b", "bad", "after_bad", "z")})
    assert finalize(root, "--unit-states", str(path)).exit_code == 0
    assert rows(root)["b"]["skip_reason"] == "condition_false"


def test_select_and_exclude_scope_the_units(root: Path) -> None:
    ran_a(root)
    r = finalize(root, "-s", "a", "-s", "z", "--exclude", "a")
    assert r.exit_code == 0, r.stderr
    got = rows(root)
    assert sorted(got) == ["a", "z"]  # a keeps its attempt file; z is the only unit in scope
    assert got["z"]["skip_reason"] == "not_run"


def test_repeating_finalize_rewrites_the_same_result(root: Path) -> None:
    ran_a(root)
    path = states(root, {"b": {"status": "failed"}, "z": {"status": "excluded"}})
    args = ("--unit-states", str(path), "--orchestrator-id", "job_id=5")
    assert finalize(root, *args).exit_code == 1
    first = results(root)
    assert finalize(root, *args).exit_code == 1
    second = results(root)
    assert second["results"] == first["results"]
    for key in ("status", "counts", "totals", "exit_code", "started_at", "plan_hash"):
        assert second["run"][key] == first["run"][key]
    assert (root / "target" / "run_results.json").is_file()


def test_finalize_runs_the_intermediate_cleanup(root: Path) -> None:
    ran_a(root)
    yml = root / "pipeloom.yml"
    yml.write_text(
        yml.read_text(encoding="utf-8") + "artifacts: {intermediate_ttl: 0}\n", encoding="utf-8"
    )
    folder = root / "target/runs/r1/intermediate"
    folder.mkdir(parents=True)
    (folder / "x.bin").write_bytes(b"x")
    assert finalize(root).exit_code == 0
    assert not folder.exists()  # ttl 0 and the run succeeded


def test_finalize_uses_a_manifest_and_checks_the_plan_hash(root: Path) -> None:
    out = root / "m.json"
    assert invoke("compile", root, "--out", str(out)).exit_code == 0, "compile"
    digest = json.loads(out.read_text(encoding="utf-8"))["metadata"]["plan_hash"]
    ok = finalize(root, "--manifest", str(out), "--expect-plan-hash", digest.upper())
    assert ok.exit_code == 0, ok.stderr
    bad = finalize(root, "--manifest", str(out), "--expect-plan-hash", "0" * 64)
    assert bad.exit_code == 1 and "PLX103" in bad.stderr


def test_run_id_is_required_and_checked(root: Path) -> None:
    assert invoke("finalize", root).exit_code == 2
    assert invoke("finalize", root, "--run-id", "bad id").exit_code == 2


def test_unit_states_file_errors_exit_2(root: Path) -> None:
    bad_schema = states(root, {}, schema="other/1")
    assert finalize(root, "--unit-states", str(bad_schema)).exit_code == 2
    for text in ("{", "[]", '{"schema": "pipeloom.unit_states/1", "units": []}'):
        (root / "s.json").write_text(text, encoding="utf-8")
        assert finalize(root, "--unit-states", str(root / "s.json")).exit_code == 2
    for unit in (
        {"status": "done"},
        "x",
        {"status": "failed", "code": 3},
        {"status": "failed", "orchestrator_ids": {"Bad": "1"}},
        {"status": "failed", "orchestrator_ids": {"k": 1}},
    ):
        path = states(root, {"a": unit})
        assert finalize(root, "--unit-states", str(path)).exit_code == 2
    assert finalize(root, "--unit-states", str(root / "missing.json")).exit_code == 2


def test_unit_states_json_matches_the_file(root: Path) -> None:
    units = {"b": {"status": "failed", "code": "PLA004", "message": "no file"}}
    ran_a(root)
    r = finalize(
        root, "--unit-states-json", json.dumps({"schema": "pipeloom.unit_states/1", "units": units})
    )
    assert r.exit_code == 1, r.stderr
    got = rows(root)
    assert got["b"]["status"] == "failed" and got["b"]["error"]["code"] == "PLA004"
    assert "no file" in r.stderr


def test_unit_states_json_content_stays_out_of_the_events(root: Path) -> None:
    doc = {
        "schema": "pipeloom.unit_states/1",
        "units": {"z": {"status": "excluded", "message": "SECRET-INLINE-TEXT"}},
    }
    r = finalize(root, "--unit-states-json", json.dumps(doc))
    assert "SECRET-INLINE-TEXT" not in r.stderr
    (started,) = [
        json.loads(ln)
        for ln in r.stderr.splitlines()
        if ln.startswith("{") and '"invocation.started"' in ln
    ]
    assert started["data"]["args"]["unit_states_json"] == "<--unit-states-json>"


def test_unit_states_json_errors_exit_2(root: Path) -> None:
    good = states(root, {})
    inline = '{"schema": "pipeloom.unit_states/1", "units": {}}'
    assert finalize(root, "--unit-states-json", "{").exit_code == 2
    assert finalize(root, "--unit-states-json", '{"schema": "other/1", "units": {}}').exit_code == 2
    assert (
        finalize(
            root,
            "--unit-states-json",
            '{"schema": "pipeloom.unit_states/1", "units": {"a": {"status": "done"}}}',
        ).exit_code
        == 2
    )
    both = finalize(root, "--unit-states", str(good), "--unit-states-json", inline)
    assert both.exit_code == 2
    assert "cannot be used together" in both.output


def test_run_started_at_flag_and_env(root: Path) -> None:
    flag = "2026-11-04T09:30:00Z"
    assert finalize(root, "--run-started-at", flag).exit_code == 0
    assert results(root)["run"]["started_at"] == "2026-11-04T09:30:00.000000Z"
    env = {"PIPELOOM_RUN_STARTED_AT": "2026-11-04T09:30:00+00:00"}
    assert finalize(root, env=env).exit_code == 0  # the same instant
    clash = finalize(root, "--run-started-at", "2026-11-05T00:00:00Z")
    assert clash.exit_code == 2 and "PLX108" in clash.stderr
    assert finalize(root, "--run-started-at", "2026-11-05T00:00:00").exit_code == 2


def test_env_vars_target_project_dir_and_log_format(root: Path) -> None:
    ran_a(root)
    env = {
        **ENV,
        "PIPELOOM_TARGET": "ci",
        "PIPELOOM_PROJECT_DIR": str(root),
        "PIPELOOM_LOG_FORMAT": "json",
    }
    r = CliRunner().invoke(main, ["finalize", "--run-id", "r1"], env=env)
    assert r.exit_code == 0, r.stderr
    assert events(r)


def test_vars_and_plugin_flags_are_accepted(root: Path) -> None:
    ran_a(root)
    r = finalize(root, "--vars", "{k: 1}", "--plugin", "tests.plugins.fakes:VAULT")
    assert r.exit_code == 0, r.stderr


def test_the_api_method(root: Path) -> None:
    from pipeloom.api import Project, ProjectOptions
    from pipeloom.graph.selectors import Selection
    from pipeloom.runtime.finalize_run import ExternalUnitState, FinalizeOptions

    ran_a(root)
    proj = Project(ProjectOptions(project_dir=root, target="ci", env={}))
    out = proj.finalize(
        FinalizeOptions(
            run_id="r1",
            selection=Selection(),
            unit_states={"b": ExternalUnitState("failed")},
        )
    )
    assert out.exit_code == 1 and out.run_results is not None


def test_finalize_options_validate_the_ids() -> None:
    from pipeloom.graph.selectors import Selection
    from pipeloom.runtime.finalize_run import FinalizeOptions

    with pytest.raises(ValueError, match="orchestrator id key"):
        FinalizeOptions(run_id="r", selection=Selection(), orchestrator_ids={"Bad": "1"})


# -- the link fallback ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "code", [errno.EPERM, errno.ENOTSUP, errno.EOPNOTSUPP, errno.EXDEV, errno.ENOSYS]
)
def test_write_json_once_falls_back_when_links_are_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, code: int
) -> None:
    def refuse(*_args: object, **_kw: object) -> None:
        raise OSError(code, "no hard links here")

    monkeypatch.setattr(os, "link", refuse)
    monkeypatch.setattr(os, "rename", refuse)  # Windows renames instead of linking
    target = tmp_path / "d" / "attempt-1.json"
    assert write_json_once(target, {"n": 1}) is True
    assert json.loads(target.read_text(encoding="utf-8")) == {"n": 1}
    assert write_json_once(target, {"n": 2}) is False  # exclusive: the first one wins
    assert json.loads(target.read_text(encoding="utf-8")) == {"n": 1}
    assert [p.name for p in target.parent.iterdir()] == ["attempt-1.json"]


def test_write_json_once_still_raises_other_os_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(*_args: object, **_kw: object) -> None:
        raise OSError(errno.EIO, "disk")

    monkeypatch.setattr(os, "link", refuse)
    monkeypatch.setattr(os, "rename", refuse)
    with pytest.raises(OSError, match="disk"):
        write_json_once(tmp_path / "a.json", {})
