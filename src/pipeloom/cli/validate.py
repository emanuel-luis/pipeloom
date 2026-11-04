"""The validate command."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._options import project_options
from pipeloom.cli._output import print_diagnostics


@click.command()
@project_options
@click.option("--warn-error", is_flag=True, help="Exit 1 on warnings too.")
def validate(project_dir: Path, vars_: dict[str, Any], warn_error: bool) -> None:
    """Check the project: YAML, schema and secrets (layers L0 and L1)."""
    opts = ProjectOptions(project_dir=project_dir, vars=vars_, env=os.environ)
    outcome = Project(opts).load()
    errors, warnings = print_diagnostics(outcome.diagnostics)
    if errors or outcome.value is None:
        click.echo(f"{errors} error(s), {warnings} warning(s)", err=True)
        raise SystemExit(1)
    if warnings and warn_error:
        click.echo(f"{warnings} warning(s) treated as errors", err=True)
        raise SystemExit(1)
    click.echo(f"Project '{outcome.value.config.name}' is valid.")
