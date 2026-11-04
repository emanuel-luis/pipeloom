"""Run each attempt on its own daemon thread in the current process (`boundary: process`).

A thread cannot be killed, so a timed-out attempt is abandoned: its `CancelToken` is set, the
engine session is asked to cancel its work once, and whatever the thread reports later is dropped.
"""

from __future__ import annotations

import contextlib
import queue
import threading
import time
from collections.abc import Callable
from typing import Literal

from pipeloom.engine.protocols import EngineSession
from pipeloom.runtime.control import CancelToken, Reason
from pipeloom.runtime.executors import UnitCompletion, UnitHandle

JOIN_TIMEOUT = 30.0
"""Seconds `shutdown(wait=True)` waits in total for attempt threads."""
RunUnit = Callable[[UnitHandle, float, CancelToken], UnitCompletion]


class ThreadExecutor:
    name: str = "thread"
    boundary: Literal["process", "task"] = "process"

    def __init__(
        self,
        run_unit: RunUnit,
        *,
        session: EngineSession | None = None,
        attempt_of: Callable[[str], int] | None = None,
        on_interrupt: Callable[[UnitHandle, Reason], None] | None = None,
    ) -> None:
        self._run_unit = run_unit
        self._session = session
        self._attempt_of = attempt_of or (lambda _unit: 1)
        self._on_interrupt = on_interrupt
        """Called once per timed-out attempt, so its attempt file exists even if the thread hangs."""
        self._done: queue.SimpleQueue[UnitCompletion | None] = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._seq = 0
        self._tokens: dict[UnitHandle, CancelToken] = {}
        self._threads: dict[UnitHandle, threading.Thread] = {}
        self._abandoned: set[UnitHandle] = set()
        self._cancelled: set[UnitHandle] = set()

    def _work(self, handle: UnitHandle, queued_at: float, token: CancelToken) -> None:
        try:
            completion = self._run_unit(handle, queued_at, token)
        except BaseException as exc:  # noqa: BLE001 - a lost completion would hang wait_any
            completion = UnitCompletion(handle, "failed", handle.attempt, internal_error=exc)
        with self._lock:
            if handle in self._abandoned:
                return  # late: the scheduler already gave this attempt a verdict
        self._done.put(completion)

    def submit(self, unit_id: str, *, queued_at: float) -> UnitHandle:
        with self._lock:
            self._seq += 1
            handle = UnitHandle(unit_id, self._seq, self._attempt_of(unit_id))
            token = self._tokens[handle] = CancelToken()
            thread = threading.Thread(
                target=self._work,
                args=(handle, queued_at, token),
                name=f"pipeloom-unit-{unit_id}-{handle.seq}",
                daemon=True,
            )
            self._threads[handle] = thread
        thread.start()
        return handle

    def wait_any(self, timeout: float | None) -> UnitCompletion | None:
        try:
            return self._done.get(timeout=timeout)
        except queue.Empty:
            return None

    def cancel(self, handle: UnitHandle, reason: Reason, *, final: bool = False) -> bool:
        final = final or reason == "timeout"
        with self._lock:
            token = self._tokens.get(handle)
            if token is None:
                return True
            if final and not token.set(reason, final=True):
                return False  # the runner already has its result; its completion is on the way
            first = handle not in self._cancelled
            self._cancelled.add(handle)
            if final:
                self._abandoned.add(handle)
        token.set(reason)
        if first and self._session is not None:
            with contextlib.suppress(
                Exception
            ):  # an engine that cannot cancel must not stop the run
                self._session.cancel(handle.unit_id)
        if final and self._on_interrupt is not None:
            self._on_interrupt(handle, reason)
        return True

    def has_terminal_event(self, handle: UnitHandle) -> bool:
        return False

    def wake(self) -> None:
        self._done.put(None)

    @property
    def abandoned_threads(self) -> int:
        """Abandoned attempts whose thread is still alive."""
        with self._lock:
            return sum(1 for h in self._abandoned if self._threads[h].is_alive())

    def shutdown(self, *, wait: bool) -> None:
        if not wait:
            # Give up on every live attempt: its late result is dropped and nobody joins it.
            with self._lock:
                gone = [
                    h for h, t in self._threads.items() if h not in self._abandoned and t.is_alive()
                ]
                self._abandoned.update(gone)
            if self._on_interrupt is not None:
                for handle in gone:
                    self._on_interrupt(handle, "cancel")
            return
        with self._lock:
            live = [t for h, t in self._threads.items() if h not in self._abandoned]
        end = time.monotonic() + JOIN_TIMEOUT
        for thread in live:  # a hung thread must not hang the run: it is a daemon
            thread.join(max(0.0, end - time.monotonic()))
