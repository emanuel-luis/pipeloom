"""The finalize command: close a run that an orchestrator drove unit by unit."""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.layout import RunPaths
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    orchestrator_context,
    orchestrator_id_option,
    plugin_option,
    project_options,
    select_options,
    target_option,
)
from pipeloom.cli.run import _plan_hash, _run_id, _started_at
from pipeloom.diagnostics import Diagnostic
from pipeloom.events.bus import EventSink
from pipeloom.events.sinks.runfile import RunFileSink
from pipeloom.graph.selectors import Selection
from pipeloom.runtime.finalize_run import FinalizeOptions, read_unit_states
from pipeloom.runtime.run import artifacts_root
from pipeloom.secrets.resolve import SecretError, secret_diagnostic


def _unit_states(ctx: click.Context, param: click.Parameter, value: Path | None) -> Path | None:
    """Check the file now (exit 2) but keep the path: the invocation event records the options,
    and a state's message may hold a secret."""
    if value is not None:
        try:
            read_unit_states(value)
        except (OSError, ValueError) as err:
            raise click.BadParameter(f"{value.as_posix()}: {err}", ctx=ctx, param=param) from err
    return value


@click.command()
@project_options
@target_option
@plugin_option
@select_options
@log_format_option
@click.option(
    "--run-id",
    callback=_run_id,
    required=True,
    help="The run to close: the run id every unit invocation used.",
)
@click.option(
    "--manifest",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Use the plan of this manifest instead of compiling the project.",
)
@click.option(
    "--expect-plan-hash",
    callback=_plan_hash,
    default=None,
    metavar="HEX",
    help="Stop with PLX103 unless the plan hash is this sha256.",
)
@click.option(
    "--unit-states",
    "unit_states",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    callback=_unit_states,
    default=None,
    metavar="PATH",
    help="A JSON file, schema pipeloom.unit_states/1, with what the orchestrator knows about "
    "units that wrote no attempt file: {unit_id: {status: skipped|cancelled|failed|excluded, "
    "reason, code, message, orchestrator_ids}}.",
)
@orchestrator_id_option
@click.option(
    "--run-started-at",
    envvar="PIPELOOM_RUN_STARTED_AT",
    callback=_started_at,
    default=None,
    metavar="ISO8601",
    help="Start time of the run, with a UTC offset. A run that recorded its start keeps it.",
)
def finalize(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    log_format: str,
    run_id: str,
    manifest: Path | None,
    expect_plan_hash: str | None,
    unit_states: Path | None,
    orchestrator_ids: dict[str, str],
    run_started_at: datetime | None,
) -> None:
    """Merge the unit results of a run into run_results.json and close the run.

    Units in scope (default: the whole plan) take their attempt file, else the state given with
    --unit-states, else they are skipped as not_run, which does not fail the run. A unit marked
    excluded gets no row. Exit codes: 0 ok, 1 a unit failed, 2 bad usage, 130 cancelled.
    Running it again rewrites the same results.
    """
    opts = ProjectOptions(
        project_dir=project_dir,
        target=target,
        vars=vars_,
        plugins=plugins,
        env=os.environ,
    )
    inv = Invocation("finalize", log_format)
    project = Project(opts, invocation_id=inv.invocation_id)
    bound = project.bind().value
    extra: list[tuple[EventSink, int]] = []
    if bound is not None:
        root = artifacts_root(bound, bound.loaded.project_dir.resolve())
        invocation = inv.invocation_id
        fallback = root / "runs" / "_invocations" / f"{invocation}.jsonl"
        extra.append((RunFileSink(lambda rid: RunPaths(root, rid).events(invocation), fallback), 5))
    try:
        bus = inv.open_bus(
            bound,
            extra=extra,
            context=orchestrator_context(orchestrator_ids),
            secrets=project.secrets,
        )
    except SecretError as err:
        inv.finish([secret_diagnostic(err)], 1)
        raise  # unreachable: finish exits
    code = 1
    diagnostics: tuple[Diagnostic, ...] = ()
    with bus:
        inv.started(bus, bound, None, None)
        try:
            outcome = project.finalize(
                FinalizeOptions(
                    run_id=run_id,
                    selection=Selection(select, exclude),
                    manifest=manifest,
                    expect_plan_hash=expect_plan_hash,
                    unit_states={} if unit_states is None else read_unit_states(unit_states),
                    orchestrator_ids=orchestrator_ids,
                    run_started_at=run_started_at,
                ),
                bus,
            )
            code, diagnostics = outcome.exit_code, outcome.diagnostics
        except KeyboardInterrupt:
            click.echo("Interrupted.", err=True)
            code = 130
        inv.completed(bus, code, diagnostics)
    if code:
        raise SystemExit(code)
