"""One invocation's events: collect during the command, emit in order at the end."""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePath
from typing import Any

import click

import pipeloom
from pipeloom.config.bind import BoundProject
from pipeloom.diagnostics import Diagnostic, Severity
from pipeloom.events.bus import EventBus, EventSink
from pipeloom.events.model import (
    ArtifactWritten,
    DiagnosticEmitted,
    EventPayload,
    InvocationCompleted,
    InvocationStarted,
)
from pipeloom.events.sinks.buffered import BufferedSink
from pipeloom.events.sinks.console import ConsoleSink
from pipeloom.events.sinks.jsonl import JsonlSink
from pipeloom.platform import SystemClock
from pipeloom.secrets.resolve import SecretResolver

TERMINAL_BUDGET = 5


def _json_arg(value: Any) -> Any:
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, tuple | list):
        return [_json_arg(v) for v in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _context_args() -> dict[str, Any]:
    """The current command's options. --vars values may hold secrets, so only names are kept."""
    ctx = click.get_current_context(silent=True)
    if ctx is None:
        return {}
    args: dict[str, Any] = {}
    for key, value in sorted(ctx.params.items()):
        if key == "vars_":
            if value:
                args["vars"] = sorted(value)
        elif value is not None and value != () and value is not False:
            args[key] = _json_arg(value)
    return args


def config_sinks(
    bound: BoundProject | None, secrets: SecretResolver | None = None
) -> list[tuple[EventSink, int]]:
    """Build the sinks of `events.sinks`, with their failure budgets, from a bound project.

    With `secrets`, `${secret:...}` references in sink options resolve (SecretError)."""
    if bound is None:
        return []
    out: list[tuple[EventSink, int]] = []
    blocks = bound.loaded.config.events.sinks
    for block, plugin in zip(blocks, bound.sinks, strict=True):
        sink: EventSink = plugin.create(secrets)
        if isinstance(sink, JsonlSink):
            sink.root = bound.loaded.project_dir
        if block.buffered:
            sink = BufferedSink(sink, block.buffer_size)
        out.append((sink, block.failure_budget))
    return out


class Invocation:
    """Collects what a command found and emits it through the bus when the command ends."""

    def __init__(self, command: str, log_format: str) -> None:
        self.command = command
        self.log_format = log_format
        self.clock = SystemClock()
        self._t0 = self.clock.monotonic()
        self.invocation_id = uuid.uuid4().hex
        """Shared with the Project (manifest metadata) and the event bus."""
        self._args = _context_args()
        self._extra: list[EventPayload] = []

    def add(self, payload: EventPayload) -> None:
        """Queue an event to emit after the diagnostics."""
        self._extra.append(payload)

    def artifact(self, path: PurePath, content: bytes, kind: str) -> None:
        import hashlib

        self._extra.append(
            ArtifactWritten(
                path=path.as_posix(),
                kind=kind,
                bytes=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
            )
        )

    def open_bus(
        self,
        bound: BoundProject | None,
        *,
        project_sinks: bool = True,
        extra: Sequence[tuple[EventSink, int]] = (),
        context: Mapping[str, str] | None = None,
        secrets: SecretResolver | None = None,
    ) -> EventBus:
        """The bus of this invocation: the terminal sink, the project's sinks, then `extra`.

        `secrets` resolves references in the sinks' options and masks the values on the bus.
        Raises SecretError."""
        terminal: EventSink = JsonlSink(None) if self.log_format == "json" else ConsoleSink()
        sinks = [(terminal, TERMINAL_BUDGET)]
        if project_sinks:
            sinks += config_sinks(bound, secrets)
        return EventBus(
            [*sinks, *extra],
            clock=self.clock,
            invocation_id=self.invocation_id,
            run_id=None,
            context=context,
            redactor=None if secrets is None else secrets.redactor,
        )

    def started(
        self, bus: EventBus, bound: BoundProject | None, project: str | None, target: str | None
    ) -> None:
        if bound is not None:
            project, target = bound.loaded.config.name, bound.target
        bus.emit(
            InvocationStarted(
                command=self.command,
                args=self._args,
                pipeloom_version=pipeloom.__version__,
                project=project,
                target=target,
            )
        )

    def completed(self, bus: EventBus, exit_code: int, diagnostics: Sequence[Diagnostic]) -> None:
        counts = {
            "errors": sum(1 for d in diagnostics if d.severity is Severity.ERROR),
            "warnings": sum(1 for d in diagnostics if d.severity is Severity.WARNING),
        }
        bus.emit(
            InvocationCompleted(
                command=self.command,
                exit_code=exit_code,
                duration_ms=int((self.clock.monotonic() - self._t0) * 1000),
                counts=counts,
            )
        )

    def finish(
        self,
        diagnostics: Sequence[Diagnostic],
        exit_code: int,
        *,
        bound: BoundProject | None = None,
        project: str | None = None,
        target: str | None = None,
    ) -> None:
        """Emit started, diagnostics, other events, completed; close the bus; exit if non-zero.

        Project-config sinks are added only when `bound` is given (a successful load and bind).
        """
        terminal: EventSink = JsonlSink(None) if self.log_format == "json" else ConsoleSink()
        sinks = [(terminal, TERMINAL_BUDGET), *config_sinks(bound)]
        if bound is not None:
            project, target = bound.loaded.config.name, bound.target
        counts: Mapping[str, int] = {
            "errors": sum(1 for d in diagnostics if d.severity is Severity.ERROR),
            "warnings": sum(1 for d in diagnostics if d.severity is Severity.WARNING),
        }
        with EventBus(
            sinks, clock=self.clock, invocation_id=self.invocation_id, run_id=None
        ) as bus:
            bus.emit(
                InvocationStarted(
                    command=self.command,
                    args=self._args,
                    pipeloom_version=pipeloom.__version__,
                    project=project,
                    target=target,
                )
            )
            for d in diagnostics:
                bus.emit(DiagnosticEmitted.from_diagnostic(d))
            for payload in self._extra:
                bus.emit(payload)
            bus.emit(
                InvocationCompleted(
                    command=self.command,
                    exit_code=exit_code,
                    duration_ms=int((self.clock.monotonic() - self._t0) * 1000),
                    counts=dict(counts),
                )
            )
        if exit_code:
            raise SystemExit(exit_code)
