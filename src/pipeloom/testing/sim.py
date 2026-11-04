"""A simulated executor: units take scripted time on a fake clock, nothing really runs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from pipeloom.runtime.control import Reason, RunControl
from pipeloom.runtime.executors import UnitCompletion, UnitHandle
from pipeloom.runtime.retry import Category
from pipeloom.testing.clock import FakeClock


@dataclass(frozen=True)
class SimAttempt:
    """One scripted attempt. `duration=None` hangs until the scheduler cancels or times it out."""

    duration: int | None
    status: Literal["succeeded", "failed"] = "succeeded"
    category: Category | None = None


def script_of(
    durations: Mapping[str, int], failures: frozenset[str] = frozenset()
) -> dict[str, list[SimAttempt]]:
    """One attempt per unit: it takes its duration and fails when listed in `failures`."""
    return {
        u: [
            SimAttempt(
                d,
                "failed" if u in failures else "succeeded",
                "permanent" if u in failures else None,
            )
        ]
        for u, d in durations.items()
    }


class SimExecutor:
    """`wait_any(timeout)` moves the clock to the earlier of the next finish and `now + timeout`.

    The clock is the scheduler's too. Give it `step=timedelta(0)` so event timestamps do not move
    simulated time. Durations are whole seconds. The nth submit of a unit plays `script[unit][n]`,
    and the last entry repeats. `cancel(handle, "cancel")` finishes the attempt as cancelled after
    `cancel_latency`; a timeout abandons it. `stop_at` calls `control.request_stop()` when the
    clock reaches that time.
    """

    name: str = "sim"
    boundary: Literal["process", "task"] = "process"

    def __init__(
        self,
        script: Mapping[str, Sequence[SimAttempt]],
        clock: FakeClock,
        *,
        cancel_latency: int = 0,
        control: RunControl | None = None,
        stop_at: int | None = None,
    ) -> None:
        if stop_at is not None and control is None:
            raise ValueError("stop_at needs a control")
        self._script = script
        self._clock = clock
        self._latency = cancel_latency
        self._control = control
        self._stop_at = stop_at
        self._seq = 0
        self._submits: Counter[str] = Counter()
        self._live: dict[UnitHandle, tuple[float | None, SimAttempt, bool]] = {}
        """Handle to (finish time or None, the scripted attempt, cancelled)."""

    def submit(self, unit_id: str, *, queued_at: float) -> UnitHandle:
        entries = self._script[unit_id]
        n = self._submits[unit_id]
        self._submits[unit_id] += 1
        entry = entries[min(n, len(entries) - 1)]
        self._seq += 1
        handle = UnitHandle(unit_id, self._seq, n + 1)
        finish = None if entry.duration is None else self._clock.monotonic() + entry.duration
        self._live[handle] = (finish, entry, False)
        return handle

    def _next(self) -> tuple[float, UnitHandle] | None:
        due = [
            (finish, h.unit_id, h.seq, h)
            for h, (finish, _, _) in self._live.items()
            if finish is not None
        ]
        if not due:
            return None
        finish, _, _, handle = min(due, key=lambda t: t[:3])
        return finish, handle

    def wait_any(self, timeout: float | None) -> UnitCompletion | None:
        now = self._clock.monotonic()
        limit = None if timeout is None else now + timeout
        nxt = self._next()
        stop = self._stop_at
        if (
            stop is not None
            and stop >= now
            and (limit is None or stop <= limit)
            and (nxt is None or stop < nxt[0])
        ):
            self._clock.advance(max(0.0, stop - now))
            assert self._control is not None
            self._control.request_stop()
            self._stop_at = None
            return None
        if nxt is not None and (limit is None or nxt[0] <= limit):
            finish, handle = nxt
            self._clock.advance(max(0.0, finish - now))
            _, entry, cancelled = self._live.pop(handle)
            if cancelled:
                return UnitCompletion(handle, "cancelled", handle.attempt, "permanent")
            category = entry.category or ("permanent" if entry.status == "failed" else None)
            return UnitCompletion(handle, entry.status, handle.attempt, category)
        if limit is None:
            raise RuntimeError("simulation deadlock: only hanging units, and no timeout")
        self._clock.advance(max(0.0, limit - now))
        return None

    def cancel(self, handle: UnitHandle, reason: Reason, *, final: bool = False) -> bool:
        live = self._live.get(handle)
        if live is None:
            return True
        if reason == "timeout" or final:
            del self._live[handle]  # abandoned: its late result is dropped
            return True
        finish, entry, _ = live
        done = self._clock.monotonic() + self._latency
        if finish is not None and finish <= done:
            return True  # it finishes before the stop could reach it
        self._live[handle] = (done, entry, True)
        return True

    def has_terminal_event(self, handle: UnitHandle) -> bool:
        return False

    def wake(self) -> None:
        pass

    def shutdown(self, *, wait: bool) -> None:
        self._live.clear()
