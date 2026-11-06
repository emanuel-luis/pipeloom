"""`pipeloom retry --from`: every flag and the acceptance check, with CliRunner."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli import main
from pipeloom.runtime import run as run_module
from tests.conftest import make_project
from tests.test_cli_run import ENV, MODELS, YML, codes, results, statuses

FIXED = MODELS.replace(
    "  - {name: bad, sql: select * from raw, inputs: {raw: {source: raw}}}\n",
    "  - {name: bad, sql: select 3 as x}\n",
)


@pytest.fixture
def root(tmp_path: Path) -> Path:
    return make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": MODELS})


def run(root: Path, *args: str) -> Result:
    argv = ["run", "--project-dir", str(root), "--target", "ci", "--log-format", "json", *args]
    return CliRunner().invoke(main, argv, env=ENV)


def retry(root: Path, *args: str, target: str = "ci") -> Result:
    argv = ["retry", "--project-dir", str(root), "--target", target, "--log-format", "json", *args]
    return CliRunner().invoke(main, argv, env=ENV)


def fix(root: Path) -> None:
    (root / "models" / "m.yml").write_text(FIXED, encoding="utf-8", newline="\n")


def run_dirs(root: Path) -> set[str]:
    return {p.name for p in (root / "target" / "runs").iterdir() if not p.name.startswith("_")}


def failed_parent(root: Path, *args: str) -> None:
    r = run(root, "--run-id", "r1", *args)
    assert r.exit_code == 1 and "run.completed" in r.stderr, r.stderr


def mark(root: Path, unit: str, status: str) -> None:
    path = root / "target" / "runs" / "r1" / "run_results.json"
    doc = json.loads(path.read_bytes().decode("utf-8"))
    for row in doc["results"]:
        if row["unit_id"] == unit:
            row["status"] = status
    path.write_bytes(json.dumps(doc).encode("utf-8"))


def test_retry_runs_the_failed_unit_and_its_skipped_descendant(root: Path) -> None:
    failed_parent(root)
    assert statuses(root, "r1")["z"] == "succeeded"
    fix(root)
    r = retry(root, "--from", "r1", "--run-id", "r2")
    assert r.exit_code == 0, r.stderr
    assert statuses(root, "r2") == {"bad": "succeeded", "after_bad": "succeeded"}
    assert results(root, "r2")["run"]["parent_run_id"] == "r1"
    bound = Project(ProjectOptions(project_dir=root, target="ci", env={})).bind().value
    assert bound is not None
    store = bound.plugins["state_stores"].create()
    record = store.get_run("r2")
    store.close()
    assert record is not None and record.parent_run_id == "r1"


def test_a_default_run_id_is_new(root: Path) -> None:
    failed_parent(root)
    fix(root)
    assert retry(root, "--from", "r1").exit_code == 0
    (new,) = run_dirs(root) - {"r1"}
    assert results(root, new)["run"]["parent_run_id"] == "r1"


def test_a_succeeded_parent_has_nothing_to_retry(root: Path) -> None:
    fix(root)
    assert run(root, "--run-id", "r1").exit_code == 0
    r = retry(root, "--from", "r1")
    assert r.exit_code == 0 and "run r1 has nothing to retry" in r.stdout
    assert run_dirs(root) == {"r1"}


def test_a_cancelled_unit_is_retried(root: Path) -> None:
    fix(root)
    assert run(root, "--run-id", "r1").exit_code == 0
    mark(root, "z", "cancelled")
    assert retry(root, "--from", "r1", "--run-id", "r2").exit_code == 0
    assert statuses(root, "r2") == {"z": "succeeded"}


def test_a_missing_run_is_plx113(root: Path) -> None:
    r = retry(root, "--from", "nope")
    assert r.exit_code == 1 and "PLX113" in codes(r)


def test_a_parent_without_results_is_plx113(root: Path) -> None:
    (root / "target" / "runs" / "r1").mkdir(parents=True)
    r = retry(root, "--from", "r1")
    assert r.exit_code == 1 and "PLX113" in codes(r)


def test_a_parent_of_another_target_is_plx113(root: Path) -> None:
    failed_parent(root)
    r = retry(root, "--from", "r1", target="other")
    assert r.exit_code == 1 and "PLX113" in codes(r)
    assert "--target ci" in r.stderr


def test_a_parent_of_another_project_is_plx113(root: Path) -> None:
    failed_parent(root)
    path = root / "target" / "runs" / "r1" / "run_results.json"
    doc = json.loads(path.read_bytes().decode("utf-8"))
    doc["run"]["project"] = "elsewhere"
    path.write_bytes(json.dumps(doc).encode("utf-8"))
    r = retry(root, "--from", "r1")
    assert r.exit_code == 1 and "PLX113" in codes(r)


def test_run_id_equal_to_from_is_a_usage_error(root: Path) -> None:
    r = retry(root, "--from", "r1", "--run-id", "r1")
    assert r.exit_code == 2 and "must differ" in r.stderr


def test_from_is_required_and_checked(root: Path) -> None:
    assert retry(root).exit_code == 2
    assert retry(root, "--from", "../x").exit_code == 2


def test_vars_are_inherited_and_the_flag_overrides(root: Path) -> None:
    failed_parent(root, "--vars", '{"k": 1, "j": 5}')
    fix(root)
    assert retry(root, "--from", "r1", "--run-id", "r2").exit_code == 0
    manifest = root / "target" / "runs" / "r2" / "manifest.json"
    assert json.loads(manifest.read_bytes())["metadata"]["vars"] == {"j": 5, "k": 1}
    r = retry(root, "--from", "r1", "--run-id", "r3", "--vars", '{"k": 2}')
    assert r.exit_code == 0, r.stderr
    manifest = root / "target" / "runs" / "r3" / "manifest.json"
    assert json.loads(manifest.read_bytes())["metadata"]["vars"] == {"j": 5, "k": 2}


@pytest.mark.parametrize(
    ("parent", "flag", "expected"),
    [
        (["--full-refresh"], [], True),
        (["--full-refresh"], ["--no-full-refresh"], False),
        ([], ["--full-refresh"], True),
        ([], [], False),
    ],
)
def test_full_refresh_is_inherited_and_the_flag_wins(
    root: Path, parent: list[str], flag: list[str], expected: bool
) -> None:
    failed_parent(root, *parent)
    fix(root)
    assert retry(root, "--from", "r1", "--run-id", "r2", *flag).exit_code == 0
    assert results(root, "r2")["args"]["full_refresh"] is expected
    assert results(root, "r2")["run"]["full_refresh"] is expected


def test_state_and_defer_are_inherited(root: Path) -> None:
    out = ["--out", "ref/manifest.json"]
    argv = ["compile", "--project-dir", str(root), "--target", "ci", "--stable", *out]
    assert CliRunner().invoke(main, argv, env=ENV).exit_code == 0
    failed_parent(root, "--state", "ref", "--defer")
    fix(root)
    assert retry(root, "--from", "r1", "--run-id", "r2").exit_code == 0
    args = results(root, "r2")["args"]
    assert args["state"] == "ref" and args["defer"] is True


@pytest.mark.parametrize(
    ("flags", "check"),
    [
        (["--fail-fast"], lambda d: d["args"]["fail_fast"] is True),
        (["--no-lock"], lambda d: d["args"]["lock"] is False),
        (["--workers", "1"], lambda d: d["args"]["workers"] == 1),
        (["--boundary", "process"], lambda d: d["args"]["boundary"] == "process"),
        (
            ["--orchestrator-id", "job_run_id=42"],
            lambda d: d["run"]["identity"]["orchestrator_ids"] == {"job_run_id": "42"},
        ),
    ],
)
def test_flags(root: Path, flags: list[str], check: Any) -> None:
    failed_parent(root)
    fix(root)
    r = retry(root, "--from", "r1", "--run-id", "r2", *flags)
    assert r.exit_code == 0, r.stderr
    assert check(results(root, "r2"))


def test_plugin_option(root: Path) -> None:
    failed_parent(root)
    (root / "pipeloom.yml").write_text(
        YML.replace("catalog: {kind: memory}", "catalog: {kind: failing}", 1),
        encoding="utf-8",
        newline="\n",
    )
    fix(root)
    r = retry(root, "--from", "r1", "--plugin", "tests.runtime.fakes:FAILING")
    assert r.exit_code == 1 and "PLM201" in r.stderr


def test_log_format_text_and_env(root: Path) -> None:
    failed_parent(root)
    fix(root)
    argv = ["retry", "--project-dir", str(root), "--target", "ci", "--from", "r1"]
    text = CliRunner().invoke(main, [*argv, "--log-format", "text"], env=ENV)
    assert text.exit_code == 0 and not text.stderr.lstrip().startswith("{")
    by_env = CliRunner().invoke(main, argv, env={**ENV, "PIPELOOM_LOG_FORMAT": "json"})
    assert by_env.exit_code == 0 and by_env.stderr.lstrip().startswith("{")


def test_workers_env_and_project_dir_env(root: Path) -> None:
    failed_parent(root)
    fix(root)
    r = CliRunner().invoke(
        main,
        ["retry", "--target", "ci", "--from", "r1", "--run-id", "r2", "--log-format", "json"],
        env={**ENV, "PIPELOOM_PROJECT_DIR": str(root), "PIPELOOM_WORKERS": "1"},
    )
    assert r.exit_code == 0, r.stderr
    assert results(root, "r2")["args"]["workers"] == 1


def test_a_retry_of_a_retry_keeps_the_chain(root: Path) -> None:
    failed_parent(root)
    r = retry(root, "--from", "r1", "--run-id", "r2")
    assert r.exit_code == 1  # still broken
    fix(root)
    r = retry(root, "--from", "r2", "--run-id", "r3")
    assert r.exit_code == 0, r.stderr
    assert results(root, "r3")["run"]["parent_run_id"] == "r2"
    assert statuses(root, "r3") == {"bad": "succeeded", "after_bad": "succeeded"}


def test_warn_error_turns_a_warning_into_exit_one(root: Path) -> None:
    failed_parent(root)
    fix(root)
    (root / "pipeloom.yml").write_text(
        YML + "model_paths: [models, missing_dir]\n", encoding="utf-8", newline="\n"
    )
    assert retry(root, "--from", "r1", "--run-id", "r2").exit_code == 0
    r = retry(root, "--from", "r1", "--run-id", "r3", "--warn-error")
    assert r.exit_code == 1 and "PLR206" in codes(r)
    assert "r3" not in run_dirs(root)


@pytest.mark.parametrize(("value", "seconds"), [("45", 45), ("30s", 30), ("2m", 120)])
def test_cancel_grace_reaches_the_run_options(
    root: Path, monkeypatch: pytest.MonkeyPatch, value: str, seconds: int
) -> None:
    failed_parent(root)
    fix(root)
    seen: list[Any] = []
    original = Project.retry

    def spy(self: Project, parent: str, opts: Any = None, *rest: Any, **kw: Any) -> Any:
        seen.append(opts.cancel_grace)
        return original(self, parent, opts, *rest, **kw)

    monkeypatch.setattr(Project, "retry", spy)
    assert retry(root, "--from", "r1", "--cancel-grace", value).exit_code == 0
    assert seen == [seconds]
    assert retry(root, "--from", "r1", "--cancel-grace", "soon").exit_code == 2


RAW = "sources:\n  - {name: raw, format: memory, location: raw/missing}\nmodels:\n"
INTERMEDIATE = (
    RAW
    + "  - {name: a, materialized: intermediate, sql: select 1 as x}\n"
    + "  - {name: c, materialized: intermediate, sql: select * from a, inputs: {a: {ref: a}}}\n"
    + "  - {name: b, sql: 'select * from c, raw', inputs: {c: {ref: c}, raw: {source: raw}}}\n"
)


def test_intermediate_parents_of_a_retried_unit_run_again(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": INTERMEDIATE})
    failed_parent(root)
    assert statuses(root, "r1") == {"a": "succeeded", "b": "failed", "c": "succeeded"}
    fixed = INTERMEDIATE.replace("'select * from c, raw'", "select * from c").replace(
        ", raw: {source: raw}", ""
    )
    (root / "models" / "m.yml").write_text(fixed, encoding="utf-8", newline="\n")
    r = retry(root, "--from", "r1", "--run-id", "r2")
    assert r.exit_code == 0, r.stderr
    assert statuses(root, "r2") == {"a": "succeeded", "b": "succeeded", "c": "succeeded"}


VAR_MODELS = MODELS + "  - {name: v, sql: 'select ${var:x} as x'}\n"


def test_vars_of_the_parent_apply_before_the_project_loads(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": VAR_MODELS})
    failed_parent(root, "--vars", '{"x": 7}')
    (root / "models" / "m.yml").write_text(
        FIXED + "  - {name: v, sql: 'select ${var:x} as x'}\n", encoding="utf-8", newline="\n"
    )
    r = retry(root, "--from", "r1", "--run-id", "r2")
    assert r.exit_code == 0, r.stderr
    assert statuses(root, "r2") == {"bad": "succeeded", "after_bad": "succeeded"}
    manifest = root / "target" / "runs" / "r2" / "manifest.json"
    assert json.loads(manifest.read_bytes())["metadata"]["vars"] == {"x": 7}


def test_unknown_parent_keeps_the_load_errors_and_hints_at_vars(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": VAR_MODELS})
    r = retry(root, "--from", "nope")
    assert r.exit_code == 1 and "PLS106" in codes(r) and "--vars" in r.stderr


DEFER_MODELS = (
    RAW
    + "  - {name: a, sql: select 1 as x}\n"
    + "  - {name: b, sql: 'select * from a, raw', inputs: {a: {ref: a}, raw: {source: raw}}}\n"
)


def test_a_retry_never_defers_what_its_parent_built(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": DEFER_MODELS})
    out = ["--out", "ref/manifest.json"]
    argv = ["compile", "--project-dir", str(root), "--target", "ci", "--stable", *out]
    assert CliRunner().invoke(main, argv, env=ENV).exit_code == 0
    failed_parent(root, "--state", "ref", "--defer")
    assert statuses(root, "r1") == {"a": "succeeded", "b": "failed"}
    built = {r["unique_id"] for r in results(root, "r1")["results"] if r["unit_id"] == "a"}
    seen: list[Any] = []
    original = run_module.deferred_relations

    def spy(*args: Any) -> Any:
        found = original(*args)
        seen.append((set(args[3]), set(found[0])))
        return found

    monkeypatch.setattr(run_module, "deferred_relations", spy)
    r = retry(root, "--from", "r1", "--run-id", "r2")
    assert statuses(root, "r2") == {"b": "failed"}, r.stderr
    assert seen == [(built, set())]  # without the rule, a would be read from the reference
    assert results(root, "r2")["args"]["defer"] is True
