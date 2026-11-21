"""The demo command: scaffold a small DuckDB project, build it and write its docs site (ADR-0041)."""

from __future__ import annotations

import os
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Final

import click

import pipeloom
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import log_format_option
from pipeloom.cli.build import build
from pipeloom.cli.docs import DEFAULT_DIR, generate
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.plugins import PluginRegistry
from pipeloom.scaffold import run_command, scaffold

ENGINE: Final = "duckdb"
INSTALL: Final = 'pip install "pipeloom[duckdb]"'
DEFAULT_FOLDER: Final = "pipeloom_demo"


def missing_engine(installed: Sequence[str]) -> Diagnostic:
    """PLU004: the demo's engine is not installed; `installed` lists the engine kinds that are."""
    found = ", ".join(sorted(installed)) or "none"
    return Report().add(
        "PLU004",
        f"the demo runs on the '{ENGINE}' engine, which is not installed "
        f"(engine plugins found: {found})",
        hint=INSTALL,
        context={"engine": ENGINE, "installed": ", ".join(sorted(installed))},
    )


def plu004(project_dir: Path) -> Sequence[Diagnostic]:
    """The case-file scenario of PLU004 (tests/cases/PLU004.yml)."""
    return [missing_engine([])]


def _engines() -> list[str]:
    """Engine kinds that are installed and not built in (the in-memory test engine does not
    count)."""
    registry = PluginRegistry.discover(env=os.environ, report=Report())
    return [e.kind for e in registry.entries("engines") if e.source != "builtin"]


def _step(command: click.Command, args: list[str]) -> None:
    """Run a command in this process; a non-zero exit stops the demo with that code."""
    try:
        command.main(args=args, prog_name="pipeloom", standalone_mode=False)
    except SystemExit as stop:
        if stop.code:
            raise


@click.command()
@click.argument("directory", required=False, type=click.Path(file_okay=False, path_type=Path))
@log_format_option
def demo(directory: Path | None, log_format: str) -> None:
    """Create a sample project on DuckDB in DIRECTORY, build it and write its docs site.

    DIRECTORY defaults to a new folder named pipeloom_demo and must be empty or missing (exit
    2 otherwise). Needs the DuckDB engine: pip install "pipeloom[duckdb]"; without it the
    command stops with PLU004 (exit 1). It never falls back to another engine.
    """
    target = directory if directory is not None else Path(DEFAULT_FOLDER)
    if target.exists() and any(target.iterdir()):
        raise click.UsageError(f"{target.as_posix()} is not empty; pass a new or empty folder")
    installed = _engines()
    if ENGINE not in installed:
        Invocation("demo", log_format).finish([missing_engine(installed)], 1)
    name = re.sub(r"[^A-Za-z0-9_]", "_", target.resolve().name) or "demo"
    name = f"p_{name}" if name[0] in "0123456789" else name
    values = {
        "name": name,
        "engine": ENGINE,
        "catalog": "",
        "version": pipeloom.__version__,
        "run_command": run_command(ENGINE),
    }
    scaffold(target, ENGINE, values)
    click.echo(f"Created the demo project '{name}' in {target.as_posix()}")
    base = ["--project-dir", str(target), "--log-format", log_format]
    _step(build, base)
    _step(generate, base)
    page = (target / DEFAULT_DIR / "index.html").as_posix()
    click.echo(f"Docs site: {page}")
    click.echo("Next steps:")
    click.echo(f"  pipeloom docs serve --project-dir {target.as_posix()}")
    click.echo(f"  pipeloom ls --project-dir {target.as_posix()}")
    click.echo("  pipeloom init --engine duckdb  # a project of your own")
