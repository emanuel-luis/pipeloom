from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.api import Project
from pipeloom.cli import main
from tests.conftest import FIXTURES, make_project

MINIMAL = FIXTURES / "minimal"
BASE = "config_version: 1\nname: shop\ntargets:\n  dev:\n    engine: {kind: memory}\n"


def run(args: list[str], env: dict[str, str] | None = None) -> Any:
    return CliRunner().invoke(main, args, env=env or {})


def test_validate_ok() -> None:
    result = run(["validate", "--project-dir", str(MINIMAL)])
    assert result.exit_code == 0
    assert result.stdout.strip() == "Project 'minimal' is valid."


def test_project_dir_env_var() -> None:
    result = run(["validate"], env={"PIPELOOM_PROJECT_DIR": str(MINIMAL)})
    assert result.exit_code == 0


def test_project_dir_flag_beats_env(tmp_path: Path) -> None:
    result = run(
        ["validate", "--project-dir", str(MINIMAL)],
        env={"PIPELOOM_PROJECT_DIR": str(tmp_path)},
    )
    assert result.exit_code == 0


def test_project_dir_must_exist(tmp_path: Path) -> None:
    assert run(["validate", "--project-dir", str(tmp_path / "nope")]).exit_code == 2


def test_missing_project_file_exits_1(tmp_path: Path) -> None:
    result = run(["validate", "--project-dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "PLY004" in result.stderr and result.stdout == ""


def test_errors_go_to_stderr_with_summary(tmp_path: Path) -> None:
    make_project(tmp_path, {"pipeloom.yml": BASE + "bogus: 1\n"})
    result = run(["validate", "--project-dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "error PLS101 pipeloom.yml:6:1" in result.stderr
    assert "1 error(s)" in result.stderr


def test_vars_valid_and_used(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {"pipeloom.yml": BASE + "defaults:\n  models: {meta: {c: '${var:country}'}}\n"},
    )
    (tmp_path / "models").mkdir()
    args = ["validate", "--project-dir", str(tmp_path)]
    assert run(args).exit_code == 1  # unknown var
    assert run([*args, "--vars", "country: BR"]).exit_code == 0
    assert run([*args, "--vars", '{"country": "BR"}']).exit_code == 0


@pytest.mark.parametrize("bad", ["[1, 2]", "just text", "a: [", "a: 1\n---\nb: 2"])
def test_vars_invalid_exits_2(bad: str) -> None:
    result = run(["validate", "--project-dir", str(MINIMAL), "--vars", bad])
    assert result.exit_code == 2
    assert "--vars" in result.stderr


def test_warn_error(tmp_path: Path) -> None:
    make_project(tmp_path, {"pipeloom.yml": BASE})  # no models dir: PLR206 warning
    args = ["validate", "--project-dir", str(tmp_path)]
    plain = run(args)
    assert plain.exit_code == 0 and "warning PLR206" in plain.stderr
    strict = run([*args, "--warn-error"])
    assert strict.exit_code == 1 and "treated as errors" in strict.stderr


def test_keyboard_interrupt_exits_130(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(self: Project) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(Project, "load", boom)
    result = run(["validate", "--project-dir", str(MINIMAL)])
    assert result.exit_code == 130
    assert "Interrupted" in result.stderr


def test_help_lists_options() -> None:
    out = run(["validate", "--help"]).stdout
    for flag in ("--project-dir", "--vars", "--warn-error"):
        assert flag in out


def test_validate_passes_the_live_environment_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    seen: list[Any] = []

    def spy(self: Project) -> Any:
        seen.append(self.options.env)
        raise KeyboardInterrupt

    monkeypatch.setattr(Project, "load", spy)
    run(["validate", "--project-dir", str(MINIMAL)])
    assert seen and seen[0] is os.environ
