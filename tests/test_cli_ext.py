"""The `pipeloom.commands` entry-point group."""

from __future__ import annotations

import json
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from tests.plugins.fakes import FakeDist, FakeEntryPoint, install_entry_points

ENV = {"PIPELOOM_TARGET": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": ""}
HELLO = "tests.extcmd:hello"


def ep(name: str, value: str, dist: str = "pipeloom-x") -> FakeEntryPoint:
    return FakeEntryPoint(name, value, "commands", FakeDist(dist, "1.0"))


def invoke(args: list[str], env: dict[str, str] | None = None) -> Any:
    return CliRunner().invoke(main, args, env={**ENV, **(env or {})})


def test_a_plugin_command_shows_in_help_and_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [ep("hello", HELLO)])
    helped = invoke(["--help"])
    assert helped.exit_code == 0 and "hello" in helped.output and "pipeloom-x" in helped.output
    result = invoke(["hello", "--name", "Ana"])
    assert result.exit_code == 0 and result.output.strip() == "hello Ana"


def test_help_lists_commands_without_importing_them(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [ep("ghost", "no_such_module_xyz:cmd")])
    helped = invoke(["--help"])
    assert helped.exit_code == 0 and "ghost" in helped.output
    assert "cannot load" not in helped.output + helped.stderr


@pytest.mark.parametrize("value", ["no_such_module_xyz:cmd", "tests.extcmd:NOT_A_COMMAND"])
def test_a_broken_command_is_a_stub_that_exits_1(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    install_entry_points(monkeypatch, [ep("ghost", value, "pipeloom-ghost")])
    result = invoke(["ghost", "--whatever", "x"])
    assert result.exit_code == 1
    assert "cannot load command 'ghost'" in result.stderr and "pipeloom-ghost" in result.stderr
    assert invoke(["--help"]).exit_code == 0


def test_a_built_in_command_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [ep("run", "tests.extcmd:shadow")])
    result = invoke(["run", "--help"])
    assert result.exit_code == 0 and "shadowing" not in result.output
    assert "built-in command has that name" in result.stderr


def test_duplicate_names_first_distribution_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(
        monkeypatch,
        [ep("hello", "tests.extcmd:shadow", "zeta"), ep("hello", HELLO, "alpha")],
    )
    result = invoke(["hello"])
    assert result.exit_code == 0 and result.stdout.strip() == "hello world"
    assert "command 'hello' from zeta is ignored" in result.stderr


def test_disabled_autoload_hides_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [ep("hello", HELLO)])
    off = {"PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": "1"}
    assert "hello" not in invoke(["--help"], off).output
    result = invoke(["hello"], off)
    assert result.exit_code == 2 and "No such command" in result.stderr


def test_plugins_list_shows_commands_without_importing(monkeypatch: pytest.MonkeyPatch) -> None:
    install_entry_points(monkeypatch, [ep("ghost", "no_such_module_xyz:cmd")])
    result = invoke(["plugins", "list", "--group", "commands", "--output", "json"])
    (row,) = json.loads(result.stdout)
    assert (
        row["group"] == "commands"
        and row["kind"] == "ghost"
        and row["distribution"] == "pipeloom-x"
    )
    checked = invoke(["plugins", "list", "--group", "commands", "--output", "json", "--check"])
    assert json.loads(checked.stdout)[0]["status"] == "PLU003"
