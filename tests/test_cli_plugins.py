from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from tests.conftest import FIXTURES, make_project
from tests.plugins.fakes import FakeDist, FakeEntryPoint, install_entry_points

MINIMAL = FIXTURES / "minimal"
FAKES = "tests.plugins.fakes"
TWO_TARGETS = (
    "config_version: 1\nname: shop\ntargets:\n"
    "  dev: {engine: {kind: memory}}\n  prod: {engine: {kind: memory}}\n"
)
NO_ENV: dict[str, str] = {"PIPELOOM_TARGET": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": ""}


def run(args: list[str], env: dict[str, str] | None = None) -> Any:
    return CliRunner().invoke(main, args, env={**NO_ENV, **(env or {})})


def two_targets(tmp_path: Path) -> list[str]:
    make_project(tmp_path, {"pipeloom.yml": TWO_TARGETS, "models/.keep": ""})
    return ["--project-dir", str(tmp_path)]


def test_validate_binds_the_target_and_plugins() -> None:
    assert run(["validate", "--project-dir", str(MINIMAL)]).exit_code == 0


def test_validate_without_a_target_choice_is_plr207(tmp_path: Path) -> None:
    result = run(["validate", *two_targets(tmp_path)])
    assert result.exit_code == 1 and "PLR207" in result.stderr


def test_target_flag(tmp_path: Path) -> None:
    args = two_targets(tmp_path)
    assert run(["validate", *args, "--target", "dev"]).exit_code == 0
    result = run(["validate", *args, "--target", "nope"])
    assert result.exit_code == 1 and "PLR204" in result.stderr


def test_target_env_var_and_flag_beats_it(tmp_path: Path) -> None:
    args = two_targets(tmp_path)
    assert run(["validate", *args], env={"PIPELOOM_TARGET": "prod"}).exit_code == 0
    bad = run(["validate", *args], env={"PIPELOOM_TARGET": "nope"})
    assert bad.exit_code == 1 and "PLR204" in bad.stderr
    ok = run(["validate", *args, "--target", "dev"], env={"PIPELOOM_TARGET": "nope"})
    assert ok.exit_code == 0


def test_plugin_flag_on_validate(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": "config_version: 1\nname: s\ntargets:\n  dev: {engine: {kind: fake}}\n",
            "models/.keep": "",
        },
    )
    args = ["validate", "--project-dir", str(tmp_path)]
    assert "PLR204" in run(args).stderr
    assert run([*args, "--plugin", f"{FAKES}:FAKE_ENGINE"]).exit_code == 0
    both = run([*args, "--plugin", f"{FAKES}:FAKE_ENGINE", "--plugin", f"{FAKES}:FAKE_SINK"])
    assert both.exit_code == 0
    assert run([*args, "--plugin", "no_such_module_xyz:X"]).stderr.count("PLU003") == 1


def test_plugins_list_text() -> None:
    result = run(["plugins", "list"])
    assert result.exit_code == 0
    lines = result.stdout.splitlines()
    assert lines[0].split() == ["GROUP", "KIND", "SOURCE", "DISTRIBUTION", "VERSION", "VALUE"]
    assert any(line.split()[:2] == ["engines", "memory"] for line in lines)
    assert "API" not in lines[0]


def test_plugins_list_group_and_json() -> None:
    result = run(["plugins", "list", "--group", "naming", "--output", "json"])
    rows = json.loads(result.stdout)
    assert [r["kind"] for r in rows] == ["none", "per_user", "prefix"]
    assert {r["group"] for r in rows} == {"naming"} and "api" not in rows[0]


def test_plugins_list_check_imports_and_reports_api() -> None:
    result = run(["plugins", "list", "--check", "--output", "json"])
    # A command entry has no plugin API version, and an installed plugin may add one.
    rows = [r for r in json.loads(result.stdout) if r["group"] != "commands"]
    assert result.exit_code == 0
    assert {r["api"] for r in rows} == {"0.4"} and {r["status"] for r in rows} == {"ok"}
    text = run(["plugins", "list", "--check"]).stdout.splitlines()[0].split()
    assert text[-2:] == ["API", "STATUS"]


def test_plugins_list_plugin_flag_and_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    ep = FakeEntryPoint("future", f"{FAKES}:API05", "engines", FakeDist("pipeloom-future", "2.0"))
    install_entry_points(monkeypatch, [ep])
    plain = run(["plugins", "list", "--group", "engines", "--output", "json"])
    assert plain.exit_code == 0
    assert [r["kind"] for r in json.loads(plain.stdout)] == ["future", "memory"]
    checked = run(["plugins", "list", "--group", "engines", "--check", "--output", "json"])
    assert checked.exit_code == 1 and "PLU001" in checked.stderr
    rows = {r["kind"]: r for r in json.loads(checked.stdout)}
    assert rows["future"]["status"] == "PLU001" and rows["future"]["api"] == "-"
    added = run(["plugins", "list", "--plugin", f"{FAKES}:FAKE_ENGINE", "--group", "engines"])
    assert "(cli)" in added.stdout and "fake" in added.stdout


def test_plugins_list_flags_duplicates_with_exit_1(monkeypatch: pytest.MonkeyPatch) -> None:
    dup = FakeEntryPoint("memory", f"{FAKES}:MEMORY_CLONE", "engines", FakeDist("other", "1"))
    install_entry_points(monkeypatch, [dup])
    result = run(["plugins", "list", "--group", "engines"])
    assert result.exit_code == 1 and "PLU002" in result.stderr
    assert result.stdout.count("memory") >= 2
    checked = json.loads(run(["plugins", "list", "--check", "--output", "json"]).stdout)
    assert {r["status"] for r in checked if r["kind"] == "memory" and r["group"] == "engines"} == {
        "PLU002"
    }


def test_autoload_env_var_hides_third_party_plugins(monkeypatch: pytest.MonkeyPatch) -> None:
    ep = FakeEntryPoint("fake", f"{FAKES}:FAKE_ENGINE", "engines", FakeDist("pipeloom-fake", "1"))
    install_entry_points(monkeypatch, [ep])
    args = ["plugins", "list", "--group", "engines", "--output", "json"]
    on = json.loads(run(args).stdout)
    off = json.loads(run(args, env={"PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "1"}).stdout)
    assert [r["kind"] for r in on] == ["fake", "memory"]
    assert [r["kind"] for r in off] == ["memory"]


def test_plugins_list_output_is_ascii(monkeypatch: pytest.MonkeyPatch) -> None:
    ep = FakeEntryPoint("fake", f"{FAKES}:FAKE_ENGINE", "engines", FakeDist("pipeloom-café", "1"))
    install_entry_points(monkeypatch, [ep])
    out = run(["plugins", "list", "--output", "json"]).stdout
    assert out.isascii() and "caf\\u00e9" in out


def test_schema_export_builtin(tmp_path: Path) -> None:
    out = tmp_path / "schemas"
    result = run(["schema", "export", "--out", str(out), "--plugins", "builtin"])
    assert result.exit_code == 0
    assert sorted(p.name for p in out.iterdir()) == ["defaults.json", "models.json", "project.json"]
    assert sorted(Path(line).name for line in result.stdout.splitlines()) == sorted(
        p.name for p in out.iterdir()
    )
    raw = (out / "project.json").read_bytes()
    assert b"\r" not in raw and raw.endswith(b"}\n")
    assert json.loads(raw)["$id"].endswith("/latest/schema/v1/project.json")


def test_schema_export_requires_out() -> None:
    assert run(["schema", "export"]).exit_code == 2


def test_schema_export_installed_includes_third_party_plugins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ep = FakeEntryPoint("fake", f"{FAKES}:FAKE_ENGINE", "engines", FakeDist("pipeloom-fake", "1"))
    install_entry_points(monkeypatch, [ep])
    installed = tmp_path / "installed"
    builtin = tmp_path / "builtin"
    assert run(["schema", "export", "--out", str(installed)]).exit_code == 0
    assert run(["schema", "export", "--out", str(builtin), "--plugins", "builtin"]).exit_code == 0
    assert '"const": "fake"' in (installed / "project.json").read_text(encoding="utf-8")
    assert '"const": "fake"' not in (builtin / "project.json").read_text(encoding="utf-8")


def test_schema_export_plugin_flag(tmp_path: Path) -> None:
    out = tmp_path / "s"
    args = ["schema", "export", "--out", str(out), "--plugins", "builtin"]
    assert run([*args, "--plugin", f"{FAKES}:FAKE_SINK"]).exit_code == 0
    assert '"const": "tap"' in (out / "project.json").read_text(encoding="utf-8")
    failed = run([*args, "--plugin", "no_such_module_xyz:X"])
    assert failed.exit_code == 1 and "PLU003" in failed.stderr


def test_schema_export_reports_plugin_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ep = FakeEntryPoint("future", f"{FAKES}:API05", "engines", FakeDist("pipeloom-future", "2"))
    install_entry_points(monkeypatch, [ep])
    result = run(["schema", "export", "--out", str(tmp_path / "o")])
    assert result.exit_code == 1 and "PLU001" in result.stderr


def test_schema_export_fails_on_duplicate_kinds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dup = FakeEntryPoint("memory", f"{FAKES}:MEMORY_CLONE", "engines", FakeDist("other", "1"))
    install_entry_points(monkeypatch, [dup])
    result = run(["schema", "export", "--out", str(tmp_path / "o")])
    assert result.exit_code == 1 and "PLU002" in result.stderr
    assert not (tmp_path / "o").exists()
