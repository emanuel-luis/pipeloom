"""The build command: unit tests first, then `run` with data tests (ADR-0045)."""

from __future__ import annotations

import click

from pipeloom.cli.run import run

assert run.callback is not None
build = click.Command(
    "build",
    callback=run.callback,  # it reads the command name to call Project.build
    params=[p for p in run.params if p.name not in ("data_tests", "backfill")],
    help="Run the unit tests of the selected models, then run them with data tests.\n\n"
    "Any failed unit test stops the build with exit 1 before a run id exists and before any "
    "write. Each model's data tests then run after its write; a failed one (PLQ303) fails the "
    "unit, skips its descendants and leaves the data written. Takes the flags of run. Exit "
    "codes: 0 ok, 1 a unit test, a unit or a check failed, 2 bad usage, 130 stopped.",
)
