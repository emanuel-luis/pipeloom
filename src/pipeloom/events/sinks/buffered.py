"""Run a slow sink on a worker thread behind a bounded queue."""

from __future__ import annotations

import threading
from collections import deque

from pipeloom.events.bus import EventSink, PlanInfo
from pipeloom.events.model import Event


class BufferedSink:
    """Queues events for a worker thread. When the queue is full the oldest event is dropped.

    `dropped` counts the drops. While the inner sink's latest attempt failed, `emit` and
    `flush` raise that error, so the bus's failure budget still applies; a later success clears
    it. `plan_ready` is queued in order with the events and is never dropped.
    """

    def __init__(self, inner: EventSink, maxsize: int = 10_000) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        self.name: str = getattr(inner, "name", type(inner).__name__)
        self._inner = inner
        self.forwarded: bool = getattr(inner, "forwarded", True)
        self._max = maxsize
        self._queue: deque[Event | PlanInfo] = deque()
        self._cond = threading.Condition()
        self._busy = False
        self._closed = False
        self._error: Exception | None = None
        self.dropped = 0
        self._worker = threading.Thread(target=self._run, name="pipeloom-sink", daemon=True)
        self._worker.start()

    def _run(self) -> None:
        while True:
            with self._cond:
                self._cond.wait_for(lambda: bool(self._queue) or self._closed)
                if not self._queue:
                    return
                item = self._queue.popleft()
                self._busy = True
            try:
                if isinstance(item, PlanInfo):
                    hook = getattr(self._inner, "plan_ready", None)
                    if hook is not None:
                        hook(item)
                else:
                    self._inner.emit(item)
                with self._cond:
                    self._error = None
            except Exception as exc:  # noqa: BLE001 - reported to the bus on the next call
                with self._cond:
                    self._error = exc
            finally:
                with self._cond:
                    self._busy = False
                    self._cond.notify_all()

    def _raise_pending(self) -> None:
        with self._cond:
            exc = self._error
        if exc is not None:
            raise exc

    def _put(self, item: Event | PlanInfo) -> None:
        # Queue first, so the inner sink keeps being tried and can recover; then report.
        with self._cond:
            if self._closed:
                raise RuntimeError("the buffered sink is closed")
            if len(self._queue) >= self._max:
                drop = next(
                    (i for i, q in enumerate(self._queue) if not isinstance(q, PlanInfo)), None
                )
                if drop is not None:
                    del self._queue[drop]
                    self.dropped += 1
            self._queue.append(item)
            self._cond.notify_all()
        self._raise_pending()

    def emit(self, event: Event) -> None:
        self._put(event)

    def plan_ready(self, info: PlanInfo) -> None:
        self._put(info)

    def flush(self) -> None:
        """Wait until the queue is drained, then flush the inner sink."""
        with self._cond:
            self._cond.wait_for(lambda: not self._queue and not self._busy)
        self._raise_pending()
        self._inner.flush()

    def close(self) -> None:
        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._cond.notify_all()
        self._worker.join()
        try:
            self._inner.flush()
        finally:
            self._inner.close()
