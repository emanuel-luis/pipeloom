"""The backfill command: run interval models over an explicit range (ADR-0054)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import click

from pipeloom.cli.run import run
from pipeloom.runtime.intervals import Backfill, to_utc

_TAKEN = frozenset(
    {
        "project_dir",
        "vars_",
        "target",
        "plugins",
        "select",
        "exclude",
        "log_format",
        "run_id",
        "run_started_at",
        "orchestrator_ids",
        "fail_fast",
        "no_lock",
        "workers",
        "boundary",
        "warn_error",
        "cancel_grace",
    }
)
"""The flags of run that backfill takes as they are."""


def _moment(ctx: click.Context, param: click.Parameter, value: str) -> datetime:
    try:
        return to_utc(datetime.fromisoformat(value))  # naive means UTC, as `begin` does
    except ValueError as err:
        raise click.BadParameter(
            f"'{value}' is not an ISO date or date-time", ctx=ctx, param=param
        ) from err


def _backfill(
    start: datetime, end: datetime, rerun: str, on_failure: str, mark_done: bool, **kwargs: Any
) -> None:
    if not kwargs["select"]:
        raise click.UsageError("backfill needs --select")
    if start >= end:
        raise click.UsageError("--start must be before --end")
    assert run.callback is not None
    run.callback(
        **kwargs,
        state=None,
        defer=False,
        manifest=None,
        expect_plan_hash=None,
        full_refresh=False,
        backfill=Backfill(start, end, rerun, on_failure, mark_done),  # type: ignore[arg-type]
    )


backfill = click.Command(
    "backfill",
    callback=_backfill,
    params=[
        *(p for p in run.params if p.name in _TAKEN),
        click.Option(
            ["--start"],
            required=True,
            callback=_moment,
            metavar="ISO",
            help="First moment of the range, inclusive: an ISO date or date-time; naive is UTC. "
            "Rounds down to the start of its window.",
        ),
        click.Option(
            ["--end"],
            required=True,
            callback=_moment,
            metavar="ISO",
            help="End of the range, exclusive. Rounds up to the end of its window.",
        ),
        click.Option(
            ["--rerun"],
            type=click.Choice(["missing", "all"]),
            default="missing",
            show_default=True,
            help="missing runs the windows the ledger does not hold; all restates the range: "
            "it removes the range from the ledger first, then runs every window.",
        ),
        click.Option(
            ["--on-failure"],
            type=click.Choice(["stop", "continue"]),
            default="stop",
            show_default=True,
            help="stop ends a model at its first failing window; continue runs the rest and "
            "reports every failed window.",
        ),
        click.Option(
            ["--mark-done"],
            is_flag=True,
            help="Record the range in the ledger exactly as given and run nothing, for a model "
            "converted from a watermark. Safe to repeat.",
        ),
    ],
    help="Run the selected interval models over [--start, --end).\n\n"
    "Each model runs the windows of its own grain that overlap the range, clipped to its "
    "begin and to the run's start, through the same path as run: one token per window, and "
    "the ledger records each window it writes. Rerunning the same backfill runs only what is "
    "missing. Models without incremental.interval run as in run when selected (with + for "
    "descendants); a selection with no interval model is PLX132. Takes the run flags except "
    "--full-refresh, --manifest, --state and --defer. Exit codes: 0 ok, 1 a window or a unit "
    "failed, 2 bad usage, 130 stopped.",
)
