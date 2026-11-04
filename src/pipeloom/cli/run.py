"""The run command: validate, plan and run the selected models."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal, cast

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.layout import RunPaths
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    select_options,
    target_option,
)
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.events.bus import EventSink
from pipeloom.events.sinks.runfile import RunFileSink
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import read_manifest
from pipeloom.runtime.executors.process import PARENT_ENV
from pipeloom.runtime.run import RunOptions, artifacts_root

_HASH = re.compile(r"^[0-9a-fA-F]{64}$")
_RUN_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _plan_hash(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
    if value is not None and not _HASH.match(value):
        raise click.BadParameter("must be 64 hexadecimal characters", ctx=ctx, param=param)
    return None if value is None else value.lower()


def _run_id(ctx: click.Context, param: click.Parameter, value: str | None) -> str | None:
    if value is not None and not _RUN_ID.match(value):
        raise click.BadParameter(
            "use 1 to 64 letters, digits, dots, underscores or hyphens", ctx=ctx, param=param
        )
    return value


@click.command()
@project_options
@target_option
@plugin_option
@select_options
@log_format_option
@click.option(
    "--manifest",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Run the plan of this manifest instead of compiling the project.",
)
@click.option(
    "--expect-plan-hash",
    callback=_plan_hash,
    default=None,
    metavar="HEX",
    help="Stop with PLX103 unless the plan hash is this sha256.",
)
@click.option(
    "--run-id",
    callback=_run_id,
    default=None,
    help="Name of the run. Reusing it continues that run. Default: a timestamp and a suffix.",
)
@click.option(
    "--full-refresh",
    is_flag=False,
    flag_value="true",
    type=click.BOOL,
    default=False,
    show_default=True,
    help="Rebuild append tables from scratch. Takes an optional true or false.",
)
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
def run(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    log_format: str,
    manifest: Path | None,
    expect_plan_hash: str | None,
    run_id: str | None,
    full_refresh: bool,
    fail_fast: bool,
    no_lock: bool,
    workers: int | None,
    boundary: str | None,
    warn_error: bool,
) -> None:
    """Run the selected models. Exit codes: 0 ok, 1 a unit or a check failed, 2 bad usage, 130 stopped.

    `-s unit:NAME` runs one unit without a lock or run results; the process executor uses it.
    """
    chosen = cast("Literal['process', 'task'] | None", boundary)
    if manifest is not None and chosen is not None:
        plan = read_manifest(manifest, Report())
        if plan is not None and plan.metadata.boundary != chosen:
            raise click.UsageError(
                f"--boundary {chosen} differs from the manifest's boundary "
                f"{plan.metadata.boundary}; compile again to change it"
            )
    opts = ProjectOptions(
        project_dir=project_dir,
        target=target,
        vars=vars_,
        plugins=plugins,
        boundary=chosen,
        env=os.environ,
    )
    inv = Invocation("run", log_format)
    project = Project(opts, invocation_id=inv.invocation_id)
    bound = project.bind().value
    parent = os.environ.get(PARENT_ENV)
    extra: list[tuple[EventSink, int]] = []
    if bound is not None:
        root = artifacts_root(bound, bound.loaded.project_dir.resolve())
        invocation = inv.invocation_id
        fallback = root / "runs" / "_invocations" / f"{invocation}.jsonl"
        extra.append((RunFileSink(lambda rid: RunPaths(root, rid).events(invocation), fallback), 5))
    bus = inv.open_bus(
        bound,
        project_sinks=parent is None,  # a child reports to its parent only
        extra=extra,
        context=None if parent is None else {"parent_invocation_id": parent},
    )
    code = 1
    diagnostics: tuple[Diagnostic, ...] = ()
    with bus:
        inv.started(bus, bound, None, None)
        try:
            outcome = project.run(
                RunOptions(
                    selection=Selection(select, exclude),
                    manifest=manifest,
                    expect_plan_hash=expect_plan_hash,
                    run_id=run_id,
                    full_refresh=full_refresh,
                    fail_fast=fail_fast,
                    lock=not no_lock,
                    workers=workers,
                    boundary=chosen,
                    warn_error=warn_error,
                ),
                bus,
            )
            code, diagnostics = outcome.exit_code, outcome.diagnostics
        except KeyboardInterrupt:  # a second Ctrl+C, after the run was finalized
            click.echo("Interrupted.", err=True)
            code = 130
        inv.completed(bus, code, diagnostics)
    if code:
        raise SystemExit(code)
