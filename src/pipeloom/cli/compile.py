"""The compile command: validate, build the plan and write the manifest."""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    select_options,
    target_option,
)
from pipeloom.diagnostics import Severity
from pipeloom.events.model import PlanCompiled
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import write_manifest
from pipeloom.plan.model import Plan

DEFAULT_OUT = "target/manifest.json"


def _summary(plan: Plan) -> PlanCompiled:
    persisted = [plan.nodes[u.nodes[-1]] for u in plan.units.values()]
    return PlanCompiled(
        project=plan.metadata.project,
        target=plan.metadata.target,
        plan_hash=plan.metadata.plan_hash,
        units=len(plan.units),
        by_materialization=dict(sorted(Counter(n.materialized for n in persisted).items())),
        ephemeral_inlined=sum(1 for n in plan.nodes.values() if n.materialized == "ephemeral"),
        selected=len(plan.selection.selected),
    )


@click.command()
@project_options
@target_option
@plugin_option
@select_options
@log_format_option
@click.option(
    "--boundary",
    type=click.Choice(["process", "task"]),
    default=None,
    help="Where units run; default is the target's boundary (process).",
)
@click.option(
    "--out",
    type=click.Path(path_type=Path),
    default=DEFAULT_OUT,
    show_default=True,
    help="Manifest file, relative to the project dir unless absolute.",
)
@click.option(
    "--stable",
    is_flag=True,
    help="Blank generated_at, invocation_id and the selection so the same project and target "
    "always give the same bytes. `run --manifest` then runs every model unless --select is given.",
)
@click.option("--warn-error", is_flag=True, help="Exit 1 on warnings too.")
def compile_(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    log_format: str,
    boundary: str | None,
    out: Path,
    stable: bool,
    warn_error: bool,
) -> None:
    """Validate the project and write the plan to a manifest file."""
    chosen = cast("Literal['process', 'task'] | None", boundary)
    opts = ProjectOptions(
        project_dir=project_dir,
        target=target,
        vars=vars_,
        boundary=chosen,
        plugins=plugins,
        env=os.environ,
    )
    inv = Invocation("compile", log_format)
    project = Project(opts, invocation_id=inv.invocation_id)
    outcome = project.compile(Selection(select, exclude))
    bound = project.bind().value
    diags = outcome.diagnostics
    if outcome.value is None and any(d.code in ("PLG304", "PLG305") for d in diags):
        inv.finish(diags, 2, bound=bound)
    warnings = sum(1 for d in diags if d.severity is Severity.WARNING)
    if outcome.value is None or (warn_error and warnings > 0):
        inv.finish(diags, 1, bound=bound)
        return
    plan = outcome.value
    path = out if out.is_absolute() else project_dir / out
    content = write_manifest(plan, path, stable=stable)
    shown = out.as_posix() if not out.is_absolute() else path.as_posix()
    inv.add(_summary(plan))
    inv.artifact(PurePosixPath(shown), content, "manifest")
    click.echo(path.as_posix())
    inv.finish(diags, 0, bound=bound)
