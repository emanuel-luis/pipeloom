"""Human-readable events on stderr, in the style of architecture section 11."""

from __future__ import annotations

from datetime import tzinfo
from typing import Any

from rich.console import Console
from rich.text import Text

from pipeloom.diagnostics import Diagnostic, Severity, format_diagnostic
from pipeloom.events.model import DiagnosticEmitted, Event, Level
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec

_STYLES = {Level.ERROR: "red", Level.WARNING: "yellow", Level.DEBUG: "dim"}


def _sinkdisabled_diagnostic(data: dict[str, Any]) -> Diagnostic:
    return Diagnostic(
        code=str(data["code"]),
        severity=Severity.WARNING,
        message=(
            f"sink '{data['sink']}' disabled after {data['failures']} consecutive "
            f"failure(s): {data['error']}"
        ),
    )


def _plan_line(d: dict[str, Any]) -> str:
    kinds = ", ".join(f"{n} {k}" for k, n in sorted(d["by_materialization"].items()))
    inlined = f"; {d['ephemeral_inlined']} ephemeral inlined" if d["ephemeral_inlined"] else ""
    return (
        f"plan: {d['units']} units ({kinds}{inlined}) | selected {d['selected']} "
        f"| hash {str(d['plan_hash'])[:12]}"
    )


class ConsoleSink:
    """Writes one line per event to stderr. Diagnostics use `format_diagnostic`."""

    name = "console"

    def __init__(self, console: Console | None = None, tz: tzinfo | None = None) -> None:
        self._console = console or Console(stderr=True)
        self._tz = tz

    def _stamp(self, event: Event) -> str:
        return event.ts.astimezone(self._tz).strftime("%H:%M:%S")

    def _line(self, event: Event) -> tuple[str, str | None]:
        """The text to print and its style."""
        d: dict[str, Any] = dict(event.data)
        style = _STYLES.get(event.level)
        if event.type == DiagnosticEmitted.TYPE:
            return format_diagnostic(Diagnostic.from_json(d)), style
        if event.type == "sink.disabled":
            return format_diagnostic(_sinkdisabled_diagnostic(d)), style
        stamp = self._stamp(event)
        if event.type == "invocation.started":
            parts = [f"pipeloom {d['pipeloom_version']}", f"command {d['command']}"]
            if d["project"]:
                parts.append(f"project {d['project']}")
            if d["target"]:
                parts.append(f"target {d['target']}")
            return f"{stamp} {' | '.join(parts)}", style
        if event.type == "plan.compiled":
            return f"{stamp} {_plan_line(d)}", style
        if event.type == "artifact.written":
            return f"{stamp} artifact: {d['path']} ({d['kind']}, {d['bytes']} bytes)", style
        if event.type == "invocation.completed":
            counts = d["counts"]
            text = (
                f"{stamp} {d['command']} finished in {d['duration_ms']} ms | exit {d['exit_code']} "
                f"| {counts.get('errors', 0)} error(s), {counts.get('warnings', 0)} warning(s)"
            )
            if d["exit_code"] and not counts.get("errors") and counts.get("warnings"):
                text += " (warnings treated as errors)"
            return text, style
        return f"{stamp} {event.type} {d}", style

    def emit(self, event: Event) -> None:
        text, style = self._line(event)
        self._console.print(
            Text(text, style=style or ""), soft_wrap=True, highlight=False, markup=False
        )

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


class ConsoleOptions(OptionsModel):
    """The console sink has no options."""


SPEC: PluginSpec[ConsoleSink] = PluginSpec(
    "console",
    "console",
    "sinks",
    API_VERSION,
    lambda _opts: ConsoleSink(),
    ConsoleOptions,
    description="Readable lines on stderr.",
)
