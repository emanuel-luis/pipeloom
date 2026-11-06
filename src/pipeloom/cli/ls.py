"""The ls command: list the selected models."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click

from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    select_options,
    state_option,
    target_option,
)
from pipeloom.cli._select import resolve_selection


@click.command()
@project_options
@target_option
@plugin_option
@select_options
@state_option
@log_format_option
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="One model name per line, or a JSON list.",
)
def ls(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    state: Path | None,
    log_format: str,
    output: str,
) -> None:
    """List the models that --select and --exclude choose (all by default)."""
    inv = Invocation("ls", log_format)
    sel = resolve_selection(inv, project_dir, vars_, target, plugins, select, exclude, state)
    models = sorted(
        (sel.resolved.project.models[uid] for uid in sel.ids), key=lambda m: m.unique_id
    )
    if output == "json":
        rows = [
            {
                "unique_id": m.unique_id,
                "name": m.name,
                "materialized": m.config.materialized,
                "tags": list(m.config.tags),
                "path": m.file,
            }
            for m in models
        ]
        click.echo(json.dumps(rows, indent=2, ensure_ascii=True))
    else:
        for m in sorted(models, key=lambda m: m.name):
            click.echo(m.name)
    inv.finish(sel.diagnostics, 0, bound=sel.bound)
