"""Cancellation primitives: the token of one attempt and the stop switch of one run."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Literal

from pipeloom.events.model import ErrorInfo

Reason = Literal["timeout", "cancel"]


class UnitInterrupted(Exception):
    """Raised by `CancelToken.check` in a unit whose attempt was timed out or cancelled."""

    def __init__(self, reason: Reason) -> None:
        super().__init__(f"unit interrupted: {reason}")
        self.reason: Reason = reason


class CancelToken:
    """One per attempt. The first reason wins; `check` is the runner's checkpoint."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._reason: Reason | None = None
        self._lock = threading.Lock()
        self._finished = False

    def set(self, reason: Reason) -> bool:
        """False when a timeout arrives after `finish`: the attempt already has its result."""
        with self._lock:
            if reason == "timeout" and self._finished:
                return False
            if self._reason is None:
                self._reason = reason
        self._event.set()
        return True

    def finish(self) -> bool:
        """The runner's point of no return. False when a timeout won the race first."""
        with self._lock:
            if self._reason == "timeout":
                return False
            self._finished = True
            return True

    def is_set(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> Reason | None:
        return self._reason

    def check(self) -> None:
        reason = self._reason
        if reason is not None:
            raise UnitInterrupted(reason)


class RunControl:
    """Thread-safe stop switch of a run. The CLI wires signals to it; API users call it.

    The first `request_stop` asks running units to finish within the grace period; a call with
    `hard=True` skips the grace.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()  # reentrant: a signal handler may fire inside a call
        self._stop = False
        self._hard = False
        self._signal: str | None = None
        self._wakers: list[Callable[[], None]] = []

    def add_waker(self, wake: Callable[[], None]) -> None:
        """Call `wake` on every stop request, so a scheduler blocked in `wait_any` returns."""
        with self._lock:
            self._wakers.append(wake)

    def remove_waker(self, wake: Callable[[], None]) -> None:
        with self._lock:
            if wake in self._wakers:
                self._wakers.remove(wake)

    def request_stop(self, *, hard: bool = False, signal_name: str | None = None) -> None:
        with self._lock:
            self._stop = True
            self._hard = self._hard or hard
            self._signal = self._signal or signal_name
            wakers = list(self._wakers)
        for wake in wakers:
            wake()

    @property
    def stop_requested(self) -> bool:
        return self._stop

    @property
    def hard(self) -> bool:
        return self._hard

    @property
    def signal_name(self) -> str | None:
        return self._signal


def interrupt_error(reason: Reason, unit_id: str, attempt: int, timeout: float | None) -> ErrorInfo:
    """The error row of an attempt the scheduler timed out (PLX102) or cancelled (PLX104)."""
    if reason == "timeout":
        limit = "its limit" if timeout is None else f"{timeout:g}s"
        return ErrorInfo(
            code="PLX102",
            category="permanent",
            message=f"unit '{unit_id}' timed out after {limit} (attempt {attempt})",
            hint="raise the model's timeout, or make the work smaller; timed-out units are not retried",
        )
    return ErrorInfo(
        code="PLX104",
        category="permanent",
        message=f"unit '{unit_id}' was cancelled (attempt {attempt})",
        hint="the run was stopped before the unit finished",
    )
