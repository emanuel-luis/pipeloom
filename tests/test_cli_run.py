"""`pipeloom run`: every flag and option value, with CliRunner."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli import main
from pipeloom.events.model import Event
from tests.conftest import make_project

YML = """config_version: 1
name: p
targets:
  ci:
    engine: {kind: memory}
    catalog: {kind: memory}
  other:
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
}


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": MODELS})


def pipeloom(
    root: Path, *args: str, env: dict[str, str] | None = None, fmt: str = "json"
) -> Result:
    argv = ["run", "--project-dir", str(root), "--target", "ci", "--log-format", fmt, *args]
    return CliRunner().invoke(main, argv, env={**ENV, **(env or {})})


def events(result: Result) -> list[Event]:
    return [
        Event.model_validate(json.loads(ln))
        for ln in result.stderr.splitlines()
        if ln.startswith("{")
    ]


def types(result: Result) -> list[str]:
    return [e.type for e in events(result)]


def results(root: Path, run_id: str) -> dict[str, Any]:
    path = root / "target" / "runs" / run_id / "run_results.json"
    doc: dict[str, Any] = json.loads(path.read_bytes().decode("utf-8"))
    return doc


def statuses(root: Path, run_id: str) -> dict[str, str]:
    return {r["unit_id"]: r["status"] for r in results(root, run_id)["results"]}


def codes(result: Result) -> set[str]:
    return {str(e.data["code"]) for e in events(result) if "code" in e.data}


def test_a_run_writes_results_and_exits_zero(root: Path) -> None:
    r = pipeloom(root, "-s", "a", "--run-id", "r1")
    assert r.exit_code == 0, r.stderr
    assert r.stdout == ""
    seen = types(r)
    assert seen[0] == "invocation.started" and seen[-1] == "invocation.completed"
    assert "run.started" in seen and "run.completed" in seen
    assert statuses(root, "r1") == {"a": "succeeded"}
    assert (root / "target" / "run_results.json").is_file()
    done = next(e for e in events(r) if e.type == "invocation.completed")
    assert done.data["exit_code"] == 0
    # the run file of this invocation holds the same stream
    (file,) = (root / "target" / "runs" / "r1" / "events").glob("*.jsonl")
    assert len(file.read_text(encoding="utf-8").splitlines()) == len(events(r))


def test_text_format(root: Path) -> None:
    r = pipeloom(root, "-s", "a", "--run-id", "r1", fmt="text")
    assert r.exit_code == 0
    assert "1/1 OK" in r.stderr and "finished in" in r.stderr and "artifacts:" in r.stderr


def test_a_failed_unit_exits_one(root: Path) -> None:
    r = pipeloom(root, "--run-id", "r1", "--workers", "1")
    assert r.exit_code == 1
    assert statuses(root, "r1") == {
        "a": "succeeded",
        "after_bad": "skipped",
        "b": "succeeded",
        "bad": "failed",
        "z": "succeeded",
    }
    assert "PLM204" in r.stderr


@pytest.mark.parametrize("flag", ["-s", "--select"])
def test_select(root: Path, flag: str) -> None:
    assert pipeloom(root, flag, "a", "--run-id", "r1").exit_code == 0
    assert statuses(root, "r1") == {"a": "succeeded"}


def test_select_with_a_graph_operator_runs_the_whole_selection(root: Path) -> None:
    assert pipeloom(root, "-s", "a+", "--run-id", "r1").exit_code == 0
    assert statuses(root, "r1") == {"a": "succeeded", "b": "succeeded"}


def test_exclude(root: Path) -> None:
    r = pipeloom(root, "-s", "a z", "--exclude", "z", "--run-id", "r1")
    assert r.exit_code == 0 and statuses(root, "r1") == {"a": "succeeded"}


def test_a_selector_that_matches_nothing_runs_nothing(root: Path) -> None:
    r = pipeloom(root, "-s", "nope")
    assert r.exit_code == 0 and "PLG303" in codes(r) and "run.started" not in types(r)


def test_a_malformed_selector_is_a_usage_error(root: Path) -> None:
    assert pipeloom(root, "-s", "a,,b").exit_code == 2


def test_unit_selection_is_unit_mode(root: Path) -> None:
    r = pipeloom(root, "-s", "unit:a", "--run-id", "r1")
    assert r.exit_code == 0
    seen = types(r)
    assert "unit.succeeded" in seen and not [t for t in seen if t.startswith("run.")]
    assert not (root / "target" / "runs" / "r1" / "run_results.json").exists()


def test_an_unknown_unit_is_plg306(root: Path) -> None:
    r = pipeloom(root, "-s", "unit:nope")
    assert r.exit_code == 1 and "PLG306" in codes(r)


def manifest(root: Path) -> Path:
    path = root / "m.json"
    r = CliRunner().invoke(
        main,
        ["compile", "--project-dir", str(root), "--target", "ci", "--out", str(path)],
        env=ENV,
    )
    assert r.exit_code == 0, r.stderr
    return path


def test_manifest(root: Path) -> None:
    path = manifest(root)
    r = pipeloom(root, "--manifest", str(path), "-s", "a", "--run-id", "r1")
    assert r.exit_code == 0, r.stderr
    assert results(root, "r1")["args"]["manifest"] == "m.json"


def test_manifest_without_a_selector_runs_the_manifests_selection(root: Path) -> None:
    path = root / "m.json"
    argv = ["compile", "--project-dir", str(root), "--target", "ci", "-s", "z", "--out", str(path)]
    assert CliRunner().invoke(main, argv, env=ENV).exit_code == 0
    assert pipeloom(root, "--manifest", str(path), "--run-id", "r1").exit_code == 0
    assert statuses(root, "r1") == {"z": "succeeded"}


def test_a_missing_manifest_is_a_usage_error(root: Path) -> None:
    assert pipeloom(root, "--manifest", str(root / "nope.json")).exit_code == 2


def test_a_broken_manifest_is_plx107(root: Path) -> None:
    (root / "m.json").write_text("{}", encoding="utf-8")
    r = pipeloom(root, "--manifest", str(root / "m.json"))
    assert r.exit_code == 1 and "PLX107" in codes(r)


def test_boundary_must_match_the_manifest(root: Path) -> None:
    path = manifest(root)
    r = pipeloom(root, "--manifest", str(path), "--boundary", "task")
    assert r.exit_code == 2 and "manifest's boundary" in r.stderr
    assert (
        pipeloom(root, "--manifest", str(path), "--boundary", "process", "-s", "a").exit_code == 0
    )


def plan_hash(root: Path) -> str:
    plan = Project(ProjectOptions(project_dir=root, target="ci", env={})).compile().value
    assert plan is not None
    return plan.metadata.plan_hash


def test_expect_plan_hash(root: Path) -> None:
    ok = pipeloom(root, "--expect-plan-hash", plan_hash(root).upper(), "-s", "a")
    assert ok.exit_code == 0
    wrong = pipeloom(root, "--expect-plan-hash", "0" * 64)
    assert wrong.exit_code == 1 and "PLX103" in codes(wrong)
    assert "run.started" not in types(wrong)


@pytest.mark.parametrize("value", ["abc", "z" * 64, "0" * 63])
def test_expect_plan_hash_must_be_64_hex(root: Path, value: str) -> None:
    assert pipeloom(root, "--expect-plan-hash", value).exit_code == 2


def test_run_id(root: Path) -> None:
    assert pipeloom(root, "-s", "a", "--run-id", "My_run-1.2").exit_code == 0
    assert (root / "target" / "runs" / "My_run-1.2").is_dir()


@pytest.mark.parametrize("value", ["a b", "a/b", "x" * 65, ""])
def test_run_id_must_be_a_plain_name(root: Path, value: str) -> None:
    assert pipeloom(root, "--run-id", value).exit_code == 2


def test_run_id_is_generated_by_default(root: Path) -> None:
    assert pipeloom(root, "-s", "a").exit_code == 0
    (run_dir,) = (root / "target" / "runs").iterdir()
    assert len(run_dir.name) == len("20260101T000000Z-abcdef")


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        ([], False),
        (["--full-refresh"], True),
        (["--full-refresh", "true"], True),
        (["--full-refresh", "false"], False),
        (["--full-refresh=TRUE"], True),
        (["--full-refresh", "0"], False),
    ],
)
def test_full_refresh(root: Path, args: list[str], expected: bool) -> None:
    r = pipeloom(root, "-s", "a", "--run-id", "r1", *args)
    assert r.exit_code == 0, r.stderr
    assert results(root, "r1")["run"]["full_refresh"] is expected
    assert results(root, "r1")["args"]["full_refresh"] is expected


def test_full_refresh_does_not_swallow_the_next_option(root: Path) -> None:
    r = pipeloom(root, "--full-refresh", "-s", "a", "--run-id", "r1")
    assert r.exit_code == 0 and statuses(root, "r1") == {"a": "succeeded"}


def test_full_refresh_rejects_other_words(root: Path) -> None:
    assert pipeloom(root, "--full-refresh", "maybe").exit_code == 2


def test_fail_fast(root: Path) -> None:
    pipeloom(root, "--workers", "1", "--run-id", "slow", "-s", "bad+ z")
    assert statuses(root, "slow")["z"] == "succeeded"
    r = pipeloom(root, "--workers", "1", "--fail-fast", "--run-id", "fast", "-s", "bad+ z")
    assert r.exit_code == 1
    got = statuses(root, "fast")
    assert got["bad"] == "failed" and got["z"] == "skipped"
    assert results(root, "fast")["args"]["fail_fast"] is True
    skip = [e for e in events(r) if e.type == "unit.skipped" and e.unit_id == "z"]
    assert skip[0].data["reason"] == "fail_fast"


def hold(root: Path) -> Any:
    bound = Project(ProjectOptions(project_dir=root, target="ci", env={})).bind().value
    assert bound is not None
    store = bound.plugins["state_stores"].create()
    return store, store.lock("p/ci", "other")


def test_the_run_lock_and_no_lock(root: Path) -> None:
    store, lock = hold(root)
    with lock:
        held = pipeloom(root, "-s", "a", "--run-id", "r1")
        assert held.exit_code == 1 and "PLX105" in codes(held)
        free = pipeloom(root, "-s", "a", "--run-id", "r2", "--no-lock")
        assert free.exit_code == 0
    store.close()
    assert results(root, "r2")["args"]["lock"] is False


def test_workers_option_and_env(root: Path) -> None:
    pipeloom(root, "-s", "a", "--run-id", "r1", "--workers", "2")
    assert results(root, "r1")["args"]["workers"] == 2
    pipeloom(root, "-s", "a", "--run-id", "r2", env={"PIPELOOM_WORKERS": "3"})
    assert results(root, "r2")["args"]["workers"] == 3
    pipeloom(root, "-s", "a", "--run-id", "r3", "--workers", "1", env={"PIPELOOM_WORKERS": "3"})
    assert results(root, "r3")["args"]["workers"] == 1


@pytest.mark.parametrize("value", ["0", "-1", "many"])
def test_workers_must_be_a_positive_integer(root: Path, value: str) -> None:
    assert pipeloom(root, "--workers", value).exit_code == 2


def test_boundary_process(root: Path) -> None:
    r = pipeloom(root, "-s", "a", "--run-id", "r1", "--boundary", "process")
    assert r.exit_code == 0 and results(root, "r1")["args"]["boundary"] == "process"


def test_boundary_task_needs_a_shared_state_store(root: Path) -> None:
    r = pipeloom(root, "-s", "a", "--boundary", "task")
    assert r.exit_code == 1 and "PLC503" in codes(r)


def test_boundary_rejects_other_words(root: Path) -> None:
    assert pipeloom(root, "--boundary", "thread").exit_code == 2


def test_warn_error(root: Path) -> None:
    assert pipeloom(root, "-s", "nope").exit_code == 0
    r = pipeloom(root, "-s", "nope", "--warn-error")
    assert r.exit_code == 1 and "PLG303" in codes(r)


def test_vars_target_and_project_dir(root: Path) -> None:
    r = pipeloom(root, "-s", "a", "--run-id", "r1", "--vars", '{"k": 1}')
    assert r.exit_code == 0
    started = next(e for e in events(r) if e.type == "invocation.started")
    assert started.data["args"]["vars"] == ["k"] and started.data["target"] == "ci"
    argv = ["run", "--project-dir", str(root), "--log-format", "json", "-s", "a"]
    # two targets and no default: the target must be named
    assert CliRunner().invoke(main, argv, env=ENV).exit_code != 0
    by_env = CliRunner().invoke(main, argv, env={**ENV, "PIPELOOM_TARGET": "other"})
    assert by_env.exit_code == 0


def test_project_dir_env(root: Path) -> None:
    r = CliRunner().invoke(
        main,
        ["run", "--target", "ci", "-s", "a", "--log-format", "json"],
        env={**ENV, "PIPELOOM_PROJECT_DIR": str(root)},
    )
    assert r.exit_code == 0, r.stderr


def test_plugin_option(root: Path) -> None:
    (root / "pipeloom.yml").write_text(
        YML.replace("catalog: {kind: memory}", "catalog: {kind: failing}", 1),
        encoding="utf-8",
        newline="\n",
    )
    argv = ["--plugin", "tests.runtime.fakes:FAILING"]
    r = pipeloom(root, "-s", "a", *argv)
    assert r.exit_code == 1 and "PLM201" in r.stderr


def test_log_format_env(root: Path) -> None:
    argv = ["run", "--project-dir", str(root), "--target", "ci", "-s", "a"]
    r = CliRunner().invoke(main, argv, env={**ENV, "PIPELOOM_LOG_FORMAT": "json"})
    assert r.exit_code == 0 and events(r)


def test_a_broken_project_exits_one_without_a_run(root: Path) -> None:
    (root / "models" / "m.yml").write_text("models: [", encoding="utf-8")
    r = pipeloom(root)
    assert r.exit_code == 1 and "PLY001" in codes(r) and "run.started" not in types(r)


def test_ctrl_c_exits_130(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from pipeloom.runtime import run as run_module

    def boom(*_a: Any, **_k: Any) -> Any:
        raise KeyboardInterrupt

    monkeypatch.setattr(run_module, "execute_run", boom)
    monkeypatch.setattr("pipeloom.api.execute_run", boom)
    r = pipeloom(root, "-s", "a")
    assert r.exit_code == 130
    assert next(e for e in events(r) if e.type == "invocation.completed").data["exit_code"] == 130


def test_a_child_reports_to_its_parent_only(root: Path) -> None:
    (root / "pipeloom.yml").write_text(
        YML + "events:\n  sinks:\n    - {kind: jsonl, path: out/events.jsonl}\n",
        encoding="utf-8",
        newline="\n",
    )
    r = pipeloom(
        root, "-s", "unit:a", "--run-id", "r1", env={"PIPELOOM_PARENT_INVOCATION_ID": "up"}
    )
    assert r.exit_code == 0
    assert not (root / "out" / "events.jsonl").exists()
    assert {e.context.get("parent_invocation_id") for e in events(r)} == {"up"}
    plain = pipeloom(root, "-s", "a", "--run-id", "r2")
    assert plain.exit_code == 0 and (root / "out" / "events.jsonl").is_file()


def test_unit_mode_text_shows_the_failure_block(
    root: Path, golden: Callable[[str, str], None]
) -> None:
    r = pipeloom(root, "-s", "unit:bad", "--run-id", "r1", fmt="text")
    assert r.exit_code == 1
    lines = [ln for ln in r.stderr.splitlines() if "FAIL" in ln or ln.startswith("         ")]
    text = "".join(re.sub(r"^\d\d:\d\d:\d\d ", "", ln) + "\n" for ln in lines)
    golden("events/console_unit.txt", re.sub(r"\d+\.\d\ds", "0.00s", text))


def grace_seen(monkeypatch: pytest.MonkeyPatch, root: Path, *args: str) -> tuple[Result, list[Any]]:
    seen: list[Any] = []
    original = Project.run

    def spy(self: Project, opts: Any = None, bus: Any = None, control: Any = None) -> Any:
        seen.append(opts.cancel_grace)
        return original(self, opts, bus, control)

    monkeypatch.setattr(Project, "run", spy)
    return pipeloom(root, *args), seen


@pytest.mark.parametrize(("value", "seconds"), [("45", 45), ("30s", 30), ("2m", 120)])
def test_cancel_grace_takes_a_duration(
    root: Path, monkeypatch: pytest.MonkeyPatch, value: str, seconds: int
) -> None:
    result, seen = grace_seen(monkeypatch, root, "-s", "a", "--cancel-grace", value)
    assert result.exit_code == 0
    assert seen == [seconds]


def test_cancel_grace_defaults_to_unset(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result, seen = grace_seen(monkeypatch, root, "-s", "a")
    assert result.exit_code == 0
    assert seen == [None]


def test_a_bad_cancel_grace_exits_2(root: Path) -> None:
    result = pipeloom(root, "-s", "a", "--cancel-grace", "soon")
    assert result.exit_code == 2
    assert "not a duration" in result.output


def test_cancel_grace_setting_is_read_from_the_project(tmp_path: Path) -> None:
    from pipeloom.runtime.run import RunOptions, _grace

    yml = YML.replace("targets:", "cancel_grace: 7s\ntargets:", 1)
    project = make_project(tmp_path, {"pipeloom.yml": yml, "models/m.yml": MODELS})
    bound = Project(ProjectOptions(project_dir=project, target="ci")).bind().value
    assert bound is not None and bound.runtime.cancel_grace == 7
    assert _grace(RunOptions(), bound) == 7.0
    assert _grace(RunOptions(cancel_grace=2), bound) == 2.0


def _recorded_start(root: Path, run_id: str) -> str:
    return str(results(root, run_id)["run"]["started_at"])


def test_run_started_at_flag_is_recorded(root: Path) -> None:
    r = pipeloom(root, "-s", "a", "--run-id", "r1", "--run-started-at", "2026-01-02T03:04:05+02:00")
    assert r.exit_code == 0, r.stderr
    assert _recorded_start(root, "r1") == "2026-01-02T01:04:05.000000Z"


def test_run_started_at_env_var(root: Path) -> None:
    env = {"PIPELOOM_RUN_STARTED_AT": "2026-01-02T03:04:05Z"}
    r = pipeloom(root, "-s", "a", "--run-id", "r1", env=env)
    assert r.exit_code == 0, r.stderr
    assert _recorded_start(root, "r1") == "2026-01-02T03:04:05.000000Z"


@pytest.mark.parametrize("value", ["2026-01-02T03:04:05", "2026-01-02", "yesterday"])
def test_run_started_at_needs_an_offset(root: Path, value: str) -> None:
    r = pipeloom(root, "--run-id", "r1", "--run-started-at", value)
    assert r.exit_code == 2
    assert not (root / "target" / "runs" / "r1").exists()


def test_run_started_at_needs_a_run_id(root: Path) -> None:
    r = pipeloom(root, "--run-started-at", "2026-01-02T03:04:05Z")
    assert r.exit_code == 2 and "--run-id" in r.stderr


def test_a_reused_run_id_keeps_its_start_and_a_mismatch_is_exit_2(root: Path) -> None:
    first = "2026-01-02T03:04:05Z"
    assert pipeloom(root, "-s", "a", "--run-id", "r1", "--run-started-at", first).exit_code == 0
    assert pipeloom(root, "-s", "a", "--run-id", "r1").exit_code == 0
    assert _recorded_start(root, "r1") == "2026-01-02T03:04:05.000000Z"
    same = pipeloom(root, "-s", "a", "--run-id", "r1", "--run-started-at", first)
    assert same.exit_code == 0
    other = pipeloom(root, "-s", "a", "--run-id", "r1", "--run-started-at", "2026-01-03T00:00:00Z")
    assert other.exit_code == 2 and codes(other) == {"PLX108"}
    assert "2026-01-02T03:04:05" in other.stderr and "2026-01-03T00:00:00" in other.stderr


VARS_YML = """config_version: 1
name: p
vars: {out: default_out}
targets:
  ci:
    engine: {kind: memory}
    catalog: {kind: memory}
    artifacts: {root: "${var:out}"}
"""


def test_manifest_vars_bind_the_project_and_explicit_vars_win(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": VARS_YML, "models/m.yml": MODELS})
    path = root / "m.json"
    argv = ["compile", "--project-dir", str(root), "--target", "ci", "--out", str(path)]
    argv += ["--vars", '{"out": "from_manifest"}']
    assert CliRunner().invoke(main, argv, env=ENV).exit_code == 0
    assert pipeloom(root, "--manifest", str(path), "-s", "a", "--run-id", "r1").exit_code == 0
    assert (root / "from_manifest/runs/r1/run_results.json").is_file()
    assert not (root / "default_out").exists()
    r = pipeloom(
        root, "--manifest", str(path), "-s", "a", "--run-id", "r2", "--vars", '{"out": "x"}'
    )
    assert r.exit_code == 0, r.stderr
    assert (root / "x/runs/r2/run_results.json").is_file()
