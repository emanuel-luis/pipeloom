"""Human-readable events on stderr, in the style of architecture section 11."""

from __future__ import annotations

import re
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


def _schema_line(d: dict[str, Any]) -> str:
    changes = ", ".join(
        f"+{c['column']} {c['new']}"
        if c["kind"] == "added"
        else f"-{c['column']}"
        if c["kind"] == "removed"
        else f"~{c['column']} {c['old']} -> {c['new']}"
        for c in d["changes"]
    )
    return f"schema changed on {d['relation']} (policy {d['policy']}, {d['action']}): {changes}"


def _quality_lines(stamp: str, model: str, d: dict[str, Any]) -> str:
    """One WARN line per warn or drop expectation that found violations."""
    lines = []
    for r in d["results"]:
        if r["action"] not in ("warn", "drop") or not r["failed"]:
            continue
        what = f"{r['name']} ({r['kind']})"
        if r["passed"] is None:
            seen = "" if r["value"] is None else f", value {r['value']}"
            lines.append(f"{stamp} WARN   {model}: {what} failed{seen}")
        elif r["action"] == "drop":
            lines.append(
                f"{stamp} WARN   {model}: {what} dropped {r['failed']} of {d['total_rows']} rows"
            )
        else:
            lines.append(
                f"{stamp} WARN   {model}: {what} failed on {r['failed']} of {d['total_rows']} rows"
            )
    return "\n".join(lines)


_QUIET = frozenset({"unit.queued", "unit.started", "model.input_read", "model.written"})
_NAME_WIDTH = 24
_INDENT = " " * 9
_FRAME = re.compile(r"^.+:\d+ in .+$")


def _size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{round(n / 1024)} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def _secs(ms: int) -> str:
    return f"{ms / 1000:.2f}s"


def _status_counts(counts: dict[str, int]) -> str:
    parts = [f"OK {counts.get('succeeded', 0)}"]
    for key, word in (
        ("failed", "FAIL"),
        ("skipped", "SKIP"),
        ("cancelled", "CANCEL"),
        ("timed_out", "TIMEOUT"),
    ):
        if counts.get(key):
            parts.append(f"{word} {counts[key]}")
    return " | ".join(parts)


class ConsoleSink:
    """Writes one line per event to stderr. Diagnostics use `format_diagnostic`.

    A run prints its header, its plan, one line per finished unit and a summary. Unit lines come
    from the terminal unit events, whether this process or a child process emitted them.
    """

    name = "console"

    def __init__(self, console: Console | None = None, tz: tzinfo | None = None) -> None:
        self._console = console or Console(stderr=True)
        self._tz = tz
        self._total: int | None = None
        self._workers = 0
        self._finished: set[str] = set()
        self._in_run = False

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
        if event.type in _QUIET:
            return "", None
        if event.type.startswith("run.") or event.type.startswith("unit."):
            return self._run_line(event, stamp, d), style
        if event.type == "model.schema_changed":
            return f"{stamp} {event.model or 'model'}: {_schema_line(d)}", style
        if event.type == "model.quality_evaluated":
            return _quality_lines(stamp, event.model or "model", d), style
        if event.type == "model.log":
            return f"{stamp} {event.model or 'model'}: {d['message']}", style
        if event.type == "artifact.written" and self._in_run:
            return "", None  # the run summary lists the artifacts
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

    def _unit_line(self, event: Event, stamp: str, d: dict[str, Any]) -> str:
        self._finished.add(event.unit_id or event.model or "")
        done = len(self._finished)  # a retried unit finishes once
        count = f"{done}/{self._total}" if self._total else str(done)
        name = f"{event.model or event.unit_id} "
        label = name + "." * max(3, _NAME_WIDTH - len(name))
        head = f"{stamp} {count} "
        if event.type == "unit.succeeded":
            kind = (
                f"{d['materialization']}/{d['strategy']}" if d["strategy"] else d["materialization"]
            )
            rows = "" if d["rows_written"] is None else f"{d['rows_written']:,} rows"
            size = "" if not d["bytes_written"] else _size(d["bytes_written"])
            return (
                f"{head}OK     {label} {kind:<16} {rows:>12} {size:>8} {_secs(d['duration_ms']):>7}"
            )
        if event.type == "unit.failed":
            err = d["error"]
            what = f"{err['code']} {err['exception_type'] or ''}".rstrip()
            line = f"{head}FAIL   {label} {what} {_secs(d['duration_ms'])}"
            if self._in_run:  # the run summary prints the block
                return line
            pad = " " * 9
            block = [f"{pad}{err['code']} {err['message']}"]
            if err.get("hint"):
                block.append(f"{pad}hint: {err['hint']}")
            frames = [t for t in err.get("trace_excerpt", [])[:-1] if _FRAME.match(t)]
            loc = err.get("location") or {}
            if frames:
                block.append(f"{pad}at {frames[-1]}")
            elif loc.get("file"):
                block.append(
                    f"{pad}at {loc['file']}" + (f":{loc['line']}" if loc.get("line") else "")
                )
            return "\n".join([line, *block])
        if event.type == "unit.skipped":
            why = f"upstream {d['upstream']} failed" if d["upstream"] else "stopped by --fail-fast"
            return f"{head}SKIP   {label} {why}"
        if event.type == "unit.timed_out":
            return (
                f"{head}TIMEOUT {label} PLX102 after {d['timeout_s']:g}s (attempt {d['attempt']})"
            )
        return f"{head}CANCEL {label} cancelled"

    def _run_line(self, event: Event, stamp: str, d: dict[str, Any]) -> str:
        if event.type == "run.started":
            self._in_run = True
            self._total = d["selected"]
            self._workers = d["workers"]
            return f"{stamp} run {event.run_id} | engine {d['engine']} {d['engine_version']}"
        if event.type == "run.compiled":
            kinds = ", ".join(f"{n} {k}" for k, n in sorted(d["by_materialization"].items()))
            inlined = (
                f"; {d['ephemeral_inlined']} ephemeral inlined" if d["ephemeral_inlined"] else ""
            )
            return (
                f"{stamp} plan: {d['units']} units ({kinds}{inlined}) | selected {d['selected']} "
                f"| workers {self._workers} | executor {d['executor']}"
            )
        if event.type == "run.completed":
            return self._summary(stamp, d)
        if event.type == "run.cancel_requested":
            by = f" by {d['signal']}" if d["signal"] else ""
            return f"{stamp} CANCEL stop requested{by}; running units have {d['grace_s']:g}s"
        if event.type == "unit.retrying":
            err = d["error"] or {}
            what = f"{err.get('code', '')} ({err.get('category', '')})".strip()
            return (
                f"{stamp} RETRY  {event.model or event.unit_id} attempt {d['attempt']} failed "
                f"{what}, next in {d['delay_ms'] / 1000:.1f}s"
            )
        if event.type in (
            "unit.succeeded",
            "unit.failed",
            "unit.skipped",
            "unit.cancelled",
            "unit.timed_out",
        ):
            return self._unit_line(event, stamp, d)
        return f"{stamp} {event.type} {d}"

    def _summary(self, stamp: str, d: dict[str, Any]) -> str:
        head = f"{stamp} finished in {_secs(d['duration_ms'])} | {_status_counts(d['counts'])}"
        lines = [f"{head} | rows written {d['rows_written']:,}"]
        for f in d["failures"]:
            lines.append(f"{_INDENT}{f['unit_id']}  {f['code']} {f['message']}")
            pad = " " * (len(_INDENT) + len(f["unit_id"]) + 2)
            if f.get("hint"):
                lines.append(f"{pad}hint: {f['hint']}")
            frames = [t for t in f.get("trace_excerpt", [])[:-1] if _FRAME.match(t)]
            loc = f.get("location") or {}
            if frames:
                lines.append(f"{pad}at {frames[-1]}")
            elif loc.get("file"):
                at = f"{loc['file']}:{loc['line']}" if loc.get("line") else loc["file"]
                lines.append(f"{pad}at {at}")
        if d["artifacts"]:
            lines.append(f"{stamp} artifacts: {', '.join(d['artifacts'])}")
        return "\n".join(lines)

    def emit(self, event: Event) -> None:
        text, style = self._line(event)
        if not text:
            return
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
