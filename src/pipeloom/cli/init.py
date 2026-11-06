"""The init command: scaffold a new project, asking for what the flags leave out."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import click

import pipeloom
from pipeloom.config.types import IDENTIFIER_PATTERN
from pipeloom.scaffold import ENGINES, run_command, scaffold

_IDENT = re.compile(IDENTIFIER_PATTERN)


def _interactive() -> bool:
    """Prompts are shown only when stdin is a terminal."""
    return sys.stdin.isatty()


def _identifier(value: str) -> str:
    if not _IDENT.match(value):
        raise click.BadParameter(
            f"'{value}' is not a valid name: use letters, digits and underscores, "
            "not starting with a digit"
        )
    return value


def _check_identifier(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
    return None if value is None else _identifier(value)


def _default_name(directory: Path | None) -> str:
    if directory is None:
        return "my_project"
    # ASCII only: names must match the config identifier pattern, and `\W` keeps letters like "ç".
    name = re.sub(r"[^A-Za-z0-9_]", "_", directory.resolve().name) or "my_project"
    return f"p_{name}" if name[0] in "0123456789" else name


@click.command()
@click.argument("directory", required=False, type=click.Path(file_okay=False, path_type=Path))
@click.option(
    "--name",
    callback=_check_identifier,
    help="Project name. Default: the directory name.",
)
@click.option(
    "--engine",
    type=click.Choice(ENGINES),
    default=None,
    help="Engine of the dev target. Default: polars.",
)
@click.option(
    "--catalog",
    callback=_check_identifier,
    help="Unity Catalog catalog of the dev target (databricks only). Default: main.",
)
@click.option("--no-input", is_flag=True, help="Never prompt: use the flags and the defaults.")
@click.option(
    "--force",
    is_flag=True,
    help="Allow a directory that is not empty. Existing files are kept, never overwritten, and "
    "listed as skipped.",
)
def init(
    directory: Path | None,
    name: str | None,
    engine: str | None,
    catalog: str | None,
    no_input: bool,
    force: bool,
) -> None:
    """Create a project in DIRECTORY (default: a new folder named after the project).

    When stdin is a terminal and --no-input is not given, asks for the values the flags leave
    out: the project name, the engine and, on databricks, the catalog.
    """
    if catalog is not None and engine not in (None, "databricks"):
        raise click.UsageError("--catalog applies only to --engine databricks")
    ask = not no_input and _interactive()
    if name is None:
        default = _default_name(directory)
        name = (
            click.prompt("Project name", default=default, value_proc=_identifier)
            if ask
            else default
        )
    if engine is None:
        engine = (
            click.prompt("Engine", type=click.Choice(ENGINES), default="polars")
            if ask
            else "polars"
        )
    if catalog is not None and engine != "databricks":
        raise click.UsageError("--catalog applies only to --engine databricks")
    if engine == "databricks" and catalog is None:
        catalog = click.prompt("Catalog", default="main", value_proc=_identifier) if ask else "main"
    target = directory if directory is not None else Path(name)
    if (target / "pipeloom.yml").exists():
        raise click.UsageError(f"{target.as_posix()} already holds a pipeloom project")
    if target.exists() and any(target.iterdir()) and not force:
        raise click.UsageError(
            f"{target.as_posix()} is not empty; pass --force to add the project's files "
            "next to the existing ones (existing files are kept)"
        )
    values = {
        "name": name,
        "engine": engine,
        "catalog": catalog or "",
        "version": pipeloom.__version__,
        "run_command": run_command(engine),
    }
    written = scaffold(target, engine, values)
    click.echo(f"Created project '{name}' ({engine}) in {target.as_posix()}")
    for rel in written.skipped:
        click.echo(f"Skipped existing file: {rel}")
    click.echo("Next steps:")
    click.echo(f"  cd {target.as_posix()}")
    click.echo("  pipeloom validate")
    click.echo(f"  {run_command(engine)}")
