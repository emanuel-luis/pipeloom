"""The synchronous in-process event bus."""

from __future__ import annotations

import contextlib
import threading
import uuid
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from types import TracebackType
from typing import Protocol, Self

from pipeloom.events.model import Event, EventPayload, Level, SinkDisabled
from pipeloom.platform import Clock
from pipeloom.secrets.redaction import Redactor


class EventSink(Protocol):
    """Receives every event. `emit` raises to signal a failure; the bus counts it.

    A sink may set a class attribute `forwarded = False` to skip events that `EventBus.forward`
    relays from another process. The default is to receive them.
    """

    def emit(self, event: Event) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


@dataclass
class _Slot:
    sink: EventSink
    budget: int
    name: str
    failures: int = 0
    disabled: bool = False
    last_error: str = ""
    forwarded: bool = True


def _emit(event: Event, sink: EventSink) -> None:
    sink.emit(event)


def _flush(sink: EventSink) -> None:
    sink.flush()


def _sink_name(sink: EventSink) -> str:
    return getattr(sink, "name", None) or type(sink).__name__


class EventBus:
    """Fans events out to sinks in the configured order.

    One lock covers sequence numbers and fan-out, so every sink sees the same order. A sink
    that raises `Exception` `failure_budget` times in a row is disabled and the
    remaining sinks get a `sink.disabled` event. `BaseException` (Ctrl+C, SystemExit)
    propagates. Sinks must not call back into the bus: the lock is not re-entrant.
    """

    def __init__(
        self,
        sinks: Sequence[tuple[EventSink, int]],
        *,
        clock: Clock,
        invocation_id: str,
        run_id: str | None = None,
        redactor: Redactor | None = None,
        context: Mapping[str, str] | None = None,
    ) -> None:
        self._slots = [
            _Slot(sink, budget, _sink_name(sink), forwarded=getattr(sink, "forwarded", True))
            for sink, budget in sinks
        ]
        self._clock = clock
        self._invocation_id = invocation_id
        self._run_id = run_id
        self._redactor = redactor
        self._context = dict(context or {})
        self._lock = threading.Lock()
        self._seq = 0
        self._closed = False

    @property
    def invocation_id(self) -> str:
        return self._invocation_id

    def set_run_id(self, run_id: str) -> None:
        """Stamp the events that follow with `run_id`; the run id is known after the bus is built."""
        with self._lock:
            self._run_id = run_id

    def _make(
        self,
        payload: EventPayload,
        level: Level | None,
        unit_id: str | None,
        model: str | None,
        attempt: int | None,
    ) -> Event:
        self._seq += 1
        event = Event(
            id=uuid.uuid4().hex,
            run_id=self._run_id,
            invocation_id=self._invocation_id,
            seq=self._seq,
            ts=self._clock.now(),
            type=payload.TYPE,
            level=level or payload.event_level(),
            unit_id=unit_id,
            model=model,
            attempt=attempt,
            data=payload.to_data(),
            context=dict(self._context),
        )
        return self._redactor.redact(event) if self._redactor else event

    def _disable(self, slot: _Slot, pending: deque[Event]) -> None:
        slot.disabled = True
        payload = SinkDisabled(sink=slot.name, failures=slot.failures, error=slot.last_error)
        pending.append(self._make(payload, None, None, None, None))

    def _failed(self, slot: _Slot, exc: Exception, pending: deque[Event]) -> None:
        slot.failures += 1
        if isinstance(exc, OSError):
            # An OS error's text carries absolute paths: name the type and the configured path.
            where = getattr(slot.sink, "location", None)
            slot.last_error = type(exc).__name__ + (f" writing {where}" if where else "")
        else:
            slot.last_error = f"{type(exc).__name__}: {exc}"
        if slot.failures >= slot.budget:
            self._disable(slot, pending)

    def _each(
        self,
        action: Callable[[EventSink], None],
        pending: deque[Event],
        *,
        resets: bool = False,
        relayed: bool = False,
    ) -> None:
        for slot in self._slots:
            if slot.disabled or (relayed and not slot.forwarded):
                continue
            try:
                action(slot.sink)
            except Exception as exc:  # noqa: BLE001 - a sink must never break the pipeline
                self._failed(slot, exc, pending)
            else:
                if resets:  # only a delivered event ends a streak of failed deliveries
                    slot.failures = 0

    def _drain(self, pending: deque[Event]) -> None:
        # Iterative on purpose: a sink.disabled event may itself disable another sink.
        while pending:
            event = pending.popleft()
            self._each(partial(_emit, event), pending, resets=True)

    def emit(
        self,
        payload: EventPayload,
        *,
        level: Level | None = None,
        unit_id: str | None = None,
        model: str | None = None,
        attempt: int | None = None,
    ) -> Event:
        """Number the event, redact it and deliver it to every enabled sink, in order."""
        with self._lock:
            if self._closed:
                raise RuntimeError("the event bus is closed")
            event = self._make(payload, level, unit_id, model, attempt)
            self._drain(deque([event]))
            return event

    def forward(self, event: Event) -> None:
        """Deliver an event another process emitted, unchanged: its `invocation_id` and `seq`
        stay. Sinks with `forwarded = False` (a run file of this invocation) do not get it."""
        with self._lock:
            if self._closed:
                raise RuntimeError("the event bus is closed")
            pending: deque[Event] = deque()
            self._each(partial(_emit, event), pending, resets=True, relayed=True)
            self._drain(pending)

    def flush(self) -> None:
        with self._lock:
            pending: deque[Event] = deque()
            self._each(_flush, pending)
            self._drain(pending)

    def close(self) -> None:
        """Flush and close every sink. Safe to call more than once."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            pending: deque[Event] = deque()
            self._each(_flush, pending)
            # A sink still failing at the end is reported before it is closed.
            for slot in self._slots:
                if not slot.disabled and slot.failures > 0:
                    self._disable(slot, pending)
            self._drain(pending)
            # Sinks disabled while closing still get closed; a failure here is not counted.
            for slot in self._slots:
                with contextlib.suppress(Exception):  # closing is best effort
                    slot.sink.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
