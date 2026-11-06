"""The event bus and run loop `run` and `retry` share."""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping

import click

from pipeloom.api import Project
from pipeloom.artifacts.layout import RunPaths
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import orchestrator_context
from pipeloom.cli._signals import install_stop_handlers
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Diagnostic
from pipeloom.events.bus import EventBus, EventSink
from pipeloom.events.sinks.runfile import RunFileSink
from pipeloom.runtime.control import RunControl
from pipeloom.runtime.executors.process import PARENT_ENV
from pipeloom.runtime.run import RunOutcome, artifacts_root
from pipeloom.secrets.resolve import SecretError, secret_diagnostic


def drive(
    inv: Invocation,
    project: Project,
    bound: BoundProject | None,
    orchestrator_ids: Mapping[str, str],
    call: Callable[[EventBus, RunControl], RunOutcome],
) -> RunOutcome | None:
    """Open the bus, call `call` under the stop handlers and report; exit with a non-zero code.

    Returns the outcome, or None after a second Ctrl+C (exit 130)."""
    parent = os.environ.get(PARENT_ENV)
    extra: list[tuple[EventSink, int]] = []
    if bound is not None:
        root = artifacts_root(bound, bound.loaded.project_dir.resolve())
        invocation = inv.invocation_id
        fallback = root / "runs" / "_invocations" / f"{invocation}.jsonl"
        extra.append((RunFileSink(lambda rid: RunPaths(root, rid).events(invocation), fallback), 5))
    try:
        bus = inv.open_bus(
            bound,
            project_sinks=parent is None,  # a child reports to its parent only
            extra=extra,
            context={
                **({} if parent is None else {"parent_invocation_id": parent}),
                **orchestrator_context(orchestrator_ids),
            },
            secrets=project.secrets,
        )
    except SecretError as err:
        inv.finish([secret_diagnostic(err)], 1)
        raise  # unreachable: finish exits
    code = 1
    diagnostics: tuple[Diagnostic, ...] = ()
    outcome: RunOutcome | None = None
    control = RunControl()
    with bus, install_stop_handlers(control):
        inv.started(bus, bound, None, None)
        try:
            outcome = call(bus, control)
            code, diagnostics = outcome.exit_code, outcome.diagnostics
        except KeyboardInterrupt:  # a second Ctrl+C, after the run was finalized
            click.echo("Interrupted.", err=True)
            code = 130
        inv.completed(bus, code, diagnostics)
    if code:
        raise SystemExit(code)
    return outcome
