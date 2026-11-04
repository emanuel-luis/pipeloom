from __future__ import annotations

import threading
from datetime import timedelta

import pytest

from pipeloom.diagnostics import Diagnostic, Severity
from pipeloom.events.bus import EventBus
from pipeloom.events.model import (
    DiagnosticEmitted,
    Event,
    InvocationCompleted,
    InvocationStarted,
    Level,
)
from pipeloom.events.sinks.buffered import BufferedSink
from pipeloom.secrets.redaction import Redactor
from pipeloom.testing import FakeClock, RecordingSink


def started(n: int = 0) -> InvocationStarted:
    return InvocationStarted(
        command=f"c{n}", args={}, pipeloom_version="0", project=None, target=None
    )


def make_bus(*sinks: tuple[object, int], **kw: object) -> EventBus:
    return EventBus(
        list(sinks),  # type: ignore[arg-type]
        clock=FakeClock(step=timedelta(milliseconds=1)),
        invocation_id="inv",
        **kw,  # type: ignore[arg-type]
    )


def test_seq_and_envelope_fields() -> None:
    rec = RecordingSink()
    with make_bus((rec, 5), context={"dag": "d"}) as bus:
        first = bus.emit(started(), unit_id="u", model="m", attempt=2)
        second = bus.emit(started(), level=Level.DEBUG)
    assert [e.seq for e in rec.events] == [1, 2] and first.seq == 1 and second.seq == 2
    assert first.invocation_id == "inv" and first.run_id is None
    assert (first.unit_id, first.model, first.attempt, first.context) == ("u", "m", 2, {"dag": "d"})
    assert first.level is Level.INFO and second.level is Level.DEBUG
    assert first.ts < second.ts and len(first.id) == 32


def test_diagnostic_level_follows_severity() -> None:
    rec = RecordingSink()
    bus = make_bus((rec, 5))
    for sev in Severity:
        bus.emit(DiagnosticEmitted.from_diagnostic(Diagnostic("PLY001", sev, "m")))
    assert [e.level for e in rec.events] == [Level.ERROR, Level.WARNING, Level.INFO]


def run_threads(bus: EventBus, threads: int, each: int) -> None:
    def work(t: int) -> None:
        for i in range(each):
            bus.emit(started(t * 1000 + i))

    pool = [threading.Thread(target=work, args=(t,)) for t in range(threads)]
    for th in pool:
        th.start()
    for th in pool:
        th.join()


@pytest.mark.parametrize("threads", [1, 8])
def test_every_sink_sees_the_same_order_with_and_without_threads(threads: int) -> None:
    a, b = RecordingSink(), RecordingSink()
    inner = RecordingSink()
    buffered = BufferedSink(inner, 10_000)
    bus = make_bus((a, 5), (b, 5), (buffered, 5))
    run_threads(bus, threads, 200 // threads)
    bus.close()
    expected = list(range(1, 200 + 1))
    assert [e.seq for e in a.events] == expected
    assert [e.seq for e in b.events] == expected
    assert [e.seq for e in inner.events] == expected
    assert [e.id for e in a.events] == [e.id for e in inner.events]


def test_fan_out_follows_the_configured_order() -> None:
    calls: list[str] = []

    class Tap(RecordingSink):
        def __init__(self, label: str) -> None:
            super().__init__()
            self.label = label

        def emit(self, event: Event) -> None:
            calls.append(self.label)

    bus = make_bus((Tap("a"), 5), (Tap("b"), 5), (Tap("c"), 5))
    bus.emit(started())
    bus.emit(started())
    assert calls == ["a", "b", "c", "a", "b", "c"]


def test_budget_disables_the_sink_and_tells_the_others() -> None:
    bad, good = RecordingSink(fail_on="*"), RecordingSink()
    bus = make_bus((bad, 3), (good, 5))
    for _ in range(5):
        bus.emit(started())
    types = good.types()
    assert types.count("sink.disabled") == 1
    # Disabled on the third failure: the event after the third `invocation.started` is the notice.
    assert types[:4] == ["invocation.started"] * 3 + ["sink.disabled"]
    notice = next(e for e in good.events if e.type == "sink.disabled")
    assert notice.level is Level.WARNING
    assert notice.data == {
        "sink": "recording",
        "failures": 3,
        "error": "RuntimeError: recording sink told to fail on invocation.started",
        "code": "PLE401",
    }
    assert bad.events == []
    assert [e.seq for e in good.events] == [1, 2, 3, 4, 5, 6]


def test_a_success_resets_the_failure_count() -> None:
    class Flaky(RecordingSink):
        def emit(self, event: Event) -> None:
            if event.seq % 3 != 0:
                raise RuntimeError("no")
            super().emit(event)

    flaky, good = Flaky(), RecordingSink()
    bus = make_bus((flaky, 3), (good, 5))
    for _ in range(12):
        bus.emit(started())
    assert "sink.disabled" not in good.types()
    assert len(flaky.events) == 4


def test_disabling_is_iterative_when_notices_fail_too() -> None:
    sinks = [RecordingSink(fail_on="*") for _ in range(30)]
    last = RecordingSink()
    bus = make_bus(*[(s, 1) for s in sinks], (last, 1))
    bus.emit(started())
    # Each sink fails on every event, including every sink.disabled notice; no recursion.
    assert last.types().count("sink.disabled") == 30


def test_base_exception_propagates() -> None:
    class Stop(RecordingSink):
        def emit(self, event: Event) -> None:
            raise KeyboardInterrupt

    bus = make_bus((Stop(), 5))
    with pytest.raises(KeyboardInterrupt):
        bus.emit(started())
    assert bus._lock.acquire(blocking=False)
    bus._lock.release()


def test_flush_and_close_are_idempotent_and_close_is_a_context_manager() -> None:
    rec = RecordingSink()
    with make_bus((rec, 5)) as bus:
        bus.emit(started())
        bus.flush()
        assert rec.flushed == 1
    assert rec.closed
    flushed = rec.flushed
    bus.close()
    bus.close()
    assert rec.flushed == flushed
    with pytest.raises(RuntimeError, match="closed"):
        bus.emit(started())


def test_flush_failures_count_against_the_budget() -> None:
    class BadFlush(RecordingSink):
        def flush(self) -> None:
            raise OSError("disk")

    good = RecordingSink()
    bus = make_bus((BadFlush(), 1), (good, 5))
    bus.flush()
    assert good.types() == ["sink.disabled"]


def test_redactor_runs_before_sinks_and_on_the_returned_event() -> None:
    r = Redactor()
    r.register("hunter2hunter2")
    rec = RecordingSink()
    bus = make_bus((rec, 5), redactor=r, context={"k": "x hunter2hunter2"})
    event = bus.emit(
        InvocationCompleted(command="hunter2hunter2", exit_code=0, duration_ms=1, counts={})
    )
    assert event.data["command"] == "***" and rec.events[0] == event
    assert event.context == {"k": "x ***"}


def test_a_clean_flush_does_not_reset_the_failure_count() -> None:
    bad, good = RecordingSink(fail_on="*"), RecordingSink()
    bus = make_bus((bad, 3), (good, 5))
    for _ in range(3):
        bus.emit(started())
        bus.flush()
    assert good.types().count("sink.disabled") == 1


def test_close_reports_a_sink_still_failing_below_its_budget() -> None:
    bad, good = RecordingSink(fail_on="*"), RecordingSink()
    bus = make_bus((bad, 5), (good, 5))
    bus.emit(started())
    bus.emit(started())
    assert "sink.disabled" not in good.types()
    bus.close()
    notice = good.events[-1]
    assert notice.type == "sink.disabled" and notice.data["failures"] == 2
    assert notice.data["error"] == "RuntimeError: recording sink told to fail on invocation.started"
    assert bad.closed and good.closed


def test_close_stays_quiet_for_a_sink_that_recovered() -> None:
    class Once(RecordingSink):
        def emit(self, event: Event) -> None:
            if event.seq == 1:
                raise RuntimeError("once")
            super().emit(event)

    good = RecordingSink()
    bus = make_bus((Once(), 5), (good, 5))
    bus.emit(started())
    bus.emit(started())
    bus.close()
    assert "sink.disabled" not in good.types()
