"""The retry command: run again what a finished run did not finish."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Literal, cast

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._drive import drive
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    orchestrator_id_option,
    plugin_option,
    project_options,
    target_option,
)
from pipeloom.cli.run import _grace, _run_id
from pipeloom.runtime.rerun import inherited_vars
from pipeloom.runtime.run import RunOptions


@click.command()
@project_options
@target_option
@plugin_option
@log_format_option
@click.option(
    "--from",
    "from_run",
    required=True,
    callback=_run_id,
    metavar="RUN_ID",
    help="The finished run to retry: its failed, skipped and cancelled units run again.",
)
@click.option(
    "--run-id",
    callback=_run_id,
    default=None,
    help="Name of the new run; it must differ from --from. Default: a timestamp and a suffix.",
)
@click.option(
    "--full-refresh/--no-full-refresh",
    default=None,
    help="Rebuild append tables from scratch. Default: what the retried run did.",
)
@orchestrator_id_option
@click.option("--fail-fast", is_flag=True, help="After the first failure, start no more units.")
@click.option("--no-lock", is_flag=True, help="Do not take the run lock.")
@click.option(
    "--workers",
    envvar="PIPELOOM_WORKERS",
    type=click.IntRange(min=1),
    default=None,
    help="Units to run at once. Default: the target's setting, else min(4, CPUs).",
)
@click.option(
    "--boundary",
    type=click.Choice(["process", "task"]),
    default=None,
    help="process runs units on threads here; task runs each in its own process. "
    "Default: the target's boundary.",
)
@click.option("--warn-error", is_flag=True, help="Exit 1 on warnings too.")
@click.option(
    "--cancel-grace",
    callback=_grace,
    default=None,
    metavar="DURATION",
    help="After a stop request (Ctrl+C, SIGTERM) or a timeout, how long running units may "
    "finish before they are killed, as 30s, 2m or 30. Default: the setting, else 30s.",
)
def retry(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    from_run: str,
    run_id: str | None,
    full_refresh: bool | None,
    orchestrator_ids: dict[str, str],
    fail_fast: bool,
    no_lock: bool,
    workers: int | None,
    boundary: str | None,
    warn_error: bool,
    cancel_grace: int | None,
) -> None:
    """Run again what a finished run did not finish. Exit codes as `run`; 1 also for PLX113.

    Selects the units of run RUN_ID that failed, timed out, were skipped or were cancelled, and
    runs them in a new run that records RUN_ID as its parent. The project is compiled again, so
    a fixed model is picked up. The retry inherits the vars (`--vars` wins), `--full-refresh`,
    `--state` and `--defer` of the run it retries.
    """
    if run_id is not None and run_id == from_run:
        raise click.UsageError("--run-id must differ from --from")
    chosen = cast("Literal['process', 'task'] | None", boundary)
    opts = ProjectOptions(
        project_dir=project_dir,
        target=target,
        vars=vars_,
        plugins=plugins,
        boundary=chosen,
        env=os.environ,
    )
    inv = Invocation("retry", log_format)
    project = Project(opts, invocation_id=inv.invocation_id)
    project = project.with_vars(inherited_vars(project, from_run))  # before the project loads
    bound = project.bind().value
    run_opts = RunOptions(
        run_id=run_id,
        orchestrator_ids=orchestrator_ids,
        fail_fast=fail_fast,
        lock=not no_lock,
        workers=workers,
        boundary=chosen,
        warn_error=warn_error,
        cancel_grace=cancel_grace,
    )
    outcome = drive(
        inv,
        project,
        bound,
        orchestrator_ids,
        lambda bus, ctl: project.retry(from_run, run_opts, bus, ctl, full_refresh=full_refresh),
    )
    if outcome is not None and outcome.run_results is None:
        click.echo(f"run {from_run} has nothing to retry")
