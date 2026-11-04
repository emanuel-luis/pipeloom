"""The schema command group."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import click

from pipeloom.cli._options import plugin_option
from pipeloom.cli._output import print_diagnostics
from pipeloom.config.schema import dump_schema, export_schemas
from pipeloom.diagnostics import Report
from pipeloom.plugins import PluginRegistry


@click.group()
def schema() -> None:
    """JSON Schemas for the YAML files."""


@schema.command("export")
@click.option(
    "--out",
    "out",
    required=True,
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to write project.json, models.json and defaults.json into.",
)
@click.option(
    "--plugins",
    "which",
    type=click.Choice(["installed", "builtin"]),
    default="installed",
    show_default=True,
    help="Plugins whose options appear in the schema; installed imports every plugin.",
)
@plugin_option
def export(out: Path, which: str, plugins: tuple[str, ...]) -> None:
    """Write the JSON Schemas, with plugin options as oneOf branches."""
    report = Report()
    registry = PluginRegistry.discover(
        cli_plugins=plugins,
        autoload=False if which == "builtin" else None,
        env=os.environ,
        report=report,
    )
    specs: Any = registry.load_all(report)
    errors, _ = print_diagnostics(report.sorted())
    if errors:
        click.echo(f"{errors} error(s)", err=True)
        raise SystemExit(1)
    out.mkdir(parents=True, exist_ok=True)
    for name, doc in sorted(export_schemas(specs).items()):
        with (out / name).open("w", encoding="utf-8", newline="\n") as fh:
            fh.write(dump_schema(doc))
        click.echo((out / name).as_posix())
