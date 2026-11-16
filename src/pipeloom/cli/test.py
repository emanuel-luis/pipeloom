"""The test command: run the unit tests of the selected models (ADR-0045)."""

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
    select_options,
    state_option,
    target_option,
)
from pipeloom.cli._select import reference_state
from pipeloom.graph.selectors import Selection


@click.command()
@project_options
@target_option
@plugin_option
@select_options
@state_option
@log_format_option
def unit_tests(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    state: Path | None,
    log_format: str,
) -> None:
    """Run the unit tests of the selected models on their given rows.

    Each case runs the model body on in-memory input rows and compares its output with the
    expected rows. Nothing is written and no run is recorded. Exit codes: 0 every case passed,
    1 a case failed or the project does not validate, 2 usage error, 130 interrupted.
    """
    opts = ProjectOptions(
        project_dir=project_dir, target=target, vars=vars_, plugins=plugins, env=os.environ
    )
    inv = Invocation("test", log_format)
    project = Project(opts, invocation_id=inv.invocation_id)
    bound = project.bind().value
    ref = (
        None
        if bound is None
        else reference_state(inv, project_dir, state, bound.loaded.config.name, bound)
    )
    try:
        outcome = project.test(Selection(select, exclude, ref))
    except KeyboardInterrupt:
        click.echo("Interrupted.", err=True)
        inv.finish([], 130, bound=bound)
        return
    for payload in outcome.events:
        inv.add(payload)
    inv.finish(outcome.diagnostics, outcome.exit_code, bound=bound, extra_errors=outcome.failed)
