"""The schema command group."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import click

from pipeloom.cli._events import Invocation
from pipeloom.cli._options import log_format_option, plugin_option
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
@log_format_option
def export(out: Path, which: str, plugins: tuple[str, ...], log_format: str) -> None:
    """Write the JSON Schemas, with plugin options as oneOf branches."""
    inv = Invocation("schema.export", log_format)
    report = Report()
    registry = PluginRegistry.discover(
        cli_plugins=plugins,
        autoload=False if which == "builtin" else None,
        env=os.environ,
        report=report,
    )
    specs: Any = registry.load_all(report)
    if report.has_errors:
        inv.finish(report.sorted(), 1)
    out.mkdir(parents=True, exist_ok=True)
    for name, doc in sorted(export_schemas(specs).items()):
        content = dump_schema(doc).encode("utf-8")
        with (out / name).open("wb") as fh:
            fh.write(content)
        inv.artifact(out / name, content, "json_schema")
        click.echo((out / name).as_posix())
    inv.finish(report.sorted(), 0)
