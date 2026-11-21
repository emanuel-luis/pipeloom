"""The plan command. Its only mode for now is `--diff` (ADR-0052)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.plan_diff import diff_plans, document, failed, render_text
from pipeloom.artifacts.reference import load_reference
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    select_options,
    state_option,
    target_option,
)
from pipeloom.cli.compile import USAGE
from pipeloom.diagnostics import Report
from pipeloom.engine.types import EngineCapabilities
from pipeloom.graph.selectors import Selection


@click.command()
@project_options
@target_option
@plugin_option
@select_options
@state_option
@log_format_option
@click.option(
    "--diff",
    is_flag=True,
    help="Compare the project with the --state manifest and sort each model's change into a "
    "category. Required: it is the only mode of plan for now.",
)
@click.option(
    "--fail-on",
    type=click.Choice(["breaking", "any", "none"]),
    default="breaking",
    show_default=True,
    help="Exit 1 when a change is breaking (breaking, indirect-breaking or removed), when "
    "anything changed, or never.",
)
@click.option(
    "--output",
    type=click.Choice(["text", "json"]),
    default="text",
    show_default=True,
    help="A table of the changed models, or a pipeloom.plan_diff/1 JSON object.",
)
def plan(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    state: Path | None,
    log_format: str,
    diff: bool,
    fail_on: str,
    output: str,
) -> None:
    """Show what a change does before it runs: with --diff, how each model differs from the
    --state reference (new, removed, breaking, non-breaking, metadata or indirect)."""
    if not diff:
        raise click.UsageError("plan needs --diff, its only mode for now")
    if state is None:
        raise click.UsageError("--diff needs --state")
    inv = Invocation("plan", log_format)
    opts = ProjectOptions(
        project_dir=project_dir, target=target, vars=vars_, plugins=plugins, env=os.environ
    )
    project = Project(opts, invocation_id=inv.invocation_id)
    bound = project.bind().value
    report = Report()
    ref = None
    if bound is not None:
        ref = load_reference(project_dir / state, bound.loaded.config.name, report, project_dir)
        if ref is None:
            inv.finish(report.sorted(), 1, bound=bound)
    outcome = project.compile(
        Selection(select, exclude, None if ref is None else ref.selector_state())
    )
    diags = outcome.diagnostics
    if outcome.value is None and any(d.code in USAGE for d in diags):
        inv.finish(diags, 2, bound=bound)
    if outcome.value is None or ref is None:
        inv.finish(diags, 1, bound=bound)
        return
    current = outcome.value
    engine = None if bound is None else bound.plugins.get("engines")
    caps = None if engine is None else engine.spec.capabilities
    dialect = caps.sql_dialect if isinstance(caps, EngineCapabilities) else None
    changes = diff_plans(ref.plan, current, dialect, report)
    chosen = set(current.selection.selected)
    everything = not select and not exclude
    changes = [
        c for c in changes if c.unique_id in chosen or (c.category == "removed" and everything)
    ]
    if output == "json":
        click.echo(json.dumps(document(current, changes, fail_on), indent=2))
    else:
        click.echo(render_text(changes))
    report.extend(diags)
    inv.finish(report.sorted(), 1 if failed(changes, fail_on) else 0, bound=bound)
