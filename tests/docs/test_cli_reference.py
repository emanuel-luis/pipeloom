"""Every command has help, every option has help, and every command is in the CLI page's table."""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import click
import pytest

from pipeloom.cli import main

ROOT = Path(__file__).resolve().parents[2]
CLI_PAGE = ROOT / "docs" / "reference" / "cli.md"
DATABRICKS = "pipeloom databricks"


def _databricks() -> click.Command:
    cli = pytest.importorskip("pipeloom_databricks.cli")
    return cli.databricks  # type: ignore[no-any-return]


def walk(cmd: click.Command, path: str) -> Iterator[tuple[str, click.Command]]:
    yield path, cmd
    if isinstance(cmd, click.Group):
        for name, sub in sorted(cmd.commands.items()):
            yield from walk(sub, f"{path} {name}")


def table_rows() -> set[str]:
    text = CLI_PAGE.read_text(encoding="utf-8")
    section = text.split("## Where each command is explained", 1)[1].split("\n## ", 1)[0]
    return set(re.findall(r"^\| `(pipeloom[^`]*)` \|", section, re.M))


def missing_help(tree: list[tuple[str, click.Command]]) -> list[str]:
    missing = []
    for path, cmd in tree:
        if not (cmd.help or "").strip():
            missing.append(path)
        for param in cmd.params:
            if isinstance(param, click.Option) and not param.hidden and not (param.help or "").strip():
                missing.append(f"{path} {param.opts[0]}")
    return missing


def is_databricks(path: str) -> bool:
    return path == DATABRICKS or path.startswith(f"{DATABRICKS} ")


def test_every_core_command_and_option_has_help() -> None:
    assert missing_help(list(walk(main, "pipeloom"))) == []


def test_every_databricks_command_and_option_has_help() -> None:
    assert missing_help(list(walk(_databricks(), DATABRICKS))) == []


def test_every_core_command_is_in_the_table() -> None:
    rows = {r for r in table_rows() if not is_databricks(r)}
    names = {path for path, _ in walk(main, "pipeloom")} - {"pipeloom"}
    assert sorted(names - rows) == []
    assert sorted(rows - names) == []


def test_every_databricks_command_is_in_the_table() -> None:
    rows = {r for r in table_rows() if is_databricks(r)}
    names = {path for path, _ in walk(_databricks(), DATABRICKS)}
    assert sorted(names - rows) == []
    assert sorted(rows - names) == []
