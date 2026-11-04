"""The validate command."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    target_option,
)
from pipeloom.diagnostics import Severity


@click.command()
@project_options
@target_option
@plugin_option
@log_format_option
@click.option("--warn-error", is_flag=True, help="Exit 1 on warnings too.")
def validate(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    warn_error: bool,
) -> None:
    """Check the project: YAML, schema, secrets, target and plugin options (L0 to L3)."""
    opts = ProjectOptions(
        project_dir=project_dir, target=target, vars=vars_, plugins=plugins, env=os.environ
    )
    inv = Invocation("validate", log_format)
    project = Project(opts)
    outcome = project.resolve()
    bound = project.bind().value
    errors = sum(1 for d in outcome.diagnostics if d.severity is Severity.ERROR)
    warnings = sum(1 for d in outcome.diagnostics if d.severity is Severity.WARNING)
    failed = errors > 0 or outcome.value is None or (warn_error and warnings > 0)
    if bound is not None and not failed:
        click.echo(f"Project '{bound.loaded.config.name}' is valid.")
    inv.finish(outcome.diagnostics, 1 if failed else 0, bound=bound)
