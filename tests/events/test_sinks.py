from __future__ import annotations

import io
import json
import threading
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from rich.console import Console

from pipeloom.diagnostics import Diagnostic, Location, Severity
from pipeloom.events.bus import EventBus
from pipeloom.events.model import (
    ArtifactWritten,
    DiagnosticEmitted,
    Event,
    InvocationCompleted,
    InvocationStarted,
    PlanCompiled,
)
from pipeloom.events.sinks.buffered import BufferedSink
from pipeloom.events.sinks.console import ConsoleSink
from pipeloom.events.sinks.jsonl import JsonlSink
from pipeloom.testing import FakeClock, RecordingSink


def sample_events(sinks: list[tuple[object, int]]) -> None:
    clock = FakeClock(datetime(2026, 3, 4, 14, 2, 11, 250000, tzinfo=UTC), timedelta(seconds=1))
    with EventBus(sinks, clock=clock, invocation_id="inv") as bus:  # type: ignore[arg-type]
        bus.emit(
            InvocationStarted(
                command="validate",
                args={"target": "dev"},
                pipeloom_version="0.1.0",
                project="shop",
                target="dev",
            )
        )
        bus.emit(
            DiagnosticEmitted.from_diagnostic(
                Diagnostic(
                    "PLS101",
                    Severity.ERROR,
                    "unknown field 'bogus'",
                    hint="did you mean 'name'?",
                    location=Location("pipeloom.yml", 6, 1, "/bogus"),
                )
            )
        )
        bus.emit(DiagnosticEmitted.from_diagnostic(Diagnostic("PLR206", Severity.WARNING, "gone")))
        bus.emit(
            PlanCompiled(
                project="shop",
                target="dev",
                plan_hash="ab" * 32,
                units=6,
                by_materialization={"table": 3, "view": 1},
                ephemeral_inlined=1,
                selected=5,
            )
        )
        bus.emit(
            ArtifactWritten(path="target/manifest.json", kind="manifest", bytes=42, sha256="0")
        )
        bus.emit(
            InvocationCompleted(
                command="validate", exit_code=1, duration_ms=12, counts={"errors": 1, "warnings": 1}
            )
        )


def test_console_golden(golden: Callable[[str, str], None]) -> None:
    out = io.StringIO()
    console = Console(file=out, width=100, color_system=None, force_terminal=False)
    sample_events([(ConsoleSink(console, tz=UTC), 5)])
    golden("events/console.txt", out.getvalue())


def test_console_prints_brackets_literally_and_does_not_wrap() -> None:
    out = io.StringIO()
    sink = ConsoleSink(Console(file=out, width=40, color_system=None), tz=UTC)
    bus = EventBus([(sink, 5)], clock=FakeClock(), invocation_id="i")
    long = "[bold]x[/bold] " + "word " * 30
    bus.emit(DiagnosticEmitted.from_diagnostic(Diagnostic("PLY001", Severity.ERROR, long)))
    assert out.getvalue() == f"error PLY001 {long}\n"


def test_console_defaults_to_the_current_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    sample_events([(ConsoleSink(), 5)])
    captured = capsys.readouterr()
    assert captured.out == "" and "error PLS101 pipeloom.yml:6:1" in captured.err


def test_jsonl_round_trip_through_event_model(capsys: pytest.CaptureFixture[str]) -> None:
    rec = RecordingSink()
    sample_events([(JsonlSink(None), 5), (rec, 5)])
    err = capsys.readouterr().err
    assert err.endswith("\n") and "\r" not in err and err.isascii()
    parsed = [Event.model_validate_json(line) for line in err.splitlines()]
    assert parsed == rec.events
    raw = json.loads(err.splitlines()[0])
    assert raw["schema"] == "pipeloom.event/1" and raw["ts"] == "2026-03-04T14:02:11.250000Z"
    assert list(raw)[:2] == ["schema", "id"]


def test_jsonl_file_is_lazy_relative_to_root_and_truncated(tmp_path: Path) -> None:
    sink = JsonlSink("out/events.jsonl", root=tmp_path)
    assert not (tmp_path / "out").exists()
    sample_events([(sink, 5)])
    target = tmp_path / "out" / "events.jsonl"
    lines = target.read_bytes().split(b"\n")
    assert len(lines) == 7 and lines[-1] == b"" and b"\r" not in target.read_bytes()
    sample_events([(JsonlSink("out/events.jsonl", root=tmp_path), 5)])
    assert len(target.read_bytes().split(b"\n")) == 7


def test_jsonl_keeps_non_ascii_safe(tmp_path: Path) -> None:
    sink = JsonlSink("e.jsonl", root=tmp_path)
    bus = EventBus([(sink, 5)], clock=FakeClock(), invocation_id="i")
    bus.emit(DiagnosticEmitted.from_diagnostic(Diagnostic("PLY001", Severity.ERROR, "café")))
    bus.close()
    line = (tmp_path / "e.jsonl").read_text(encoding="utf-8").strip()
    assert line.isascii() and Event.model_validate_json(line).data["message"] == "café"


class Gate(RecordingSink):
    """Blocks the first emit until released, so the buffer fills up behind it."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def emit(self, event: Event) -> None:
        self.entered.set()
        self.release.wait(10)
        super().emit(event)


def test_buffered_drops_the_oldest_and_counts() -> None:
    gate = Gate()
    buffered = BufferedSink(gate, maxsize=3)
    bus = EventBus([(buffered, 5)], clock=FakeClock(), invocation_id="i")
    bus.emit(
        sample := InvocationStarted(
            command="a", args={}, pipeloom_version="0", project=None, target=None
        )
    )
    assert gate.entered.wait(10)
    for _ in range(6):
        bus.emit(sample)  # seq 2..7 while the worker holds seq 1
    assert buffered.dropped == 3
    gate.release.set()
    bus.close()
    assert [e.seq for e in gate.events] == [1, 5, 6, 7]
    assert gate.closed


def test_buffered_flush_drains_and_close_is_idempotent() -> None:
    inner = RecordingSink()
    buffered = BufferedSink(inner, maxsize=100)
    bus = EventBus([(buffered, 5)], clock=FakeClock(), invocation_id="i")
    for _ in range(20):
        bus.emit(
            InvocationStarted(command="a", args={}, pipeloom_version="0", project=None, target=None)
        )
    bus.flush()
    assert len(inner.events) == 20 and inner.flushed == 1 and buffered.dropped == 0
    buffered.close()
    buffered.close()
    with pytest.raises(RuntimeError):
        buffered.emit(inner.events[0])


def test_buffered_inner_errors_reach_the_bus_budget() -> None:
    buffered = BufferedSink(RecordingSink(fail_on="*"), maxsize=10)
    good = RecordingSink()
    bus = EventBus([(buffered, 2), (good, 5)], clock=FakeClock(), invocation_id="i")
    sample = InvocationStarted(
        command="a", args={}, pipeloom_version="0", project=None, target=None
    )
    for _ in range(4):
        bus.emit(sample)
        bus.flush()
    bus.close()
    assert good.types().count("sink.disabled") == 1
