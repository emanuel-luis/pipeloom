"""A simulated executor: units take scripted time on a fake clock, nothing really runs."""

from __future__ import annotations

import heapq
from collections.abc import Mapping
from typing import Literal

from pipeloom.runtime.executors import UnitCompletion, UnitHandle
from pipeloom.testing.clock import FakeClock


class SimExecutor:
    """`wait_any` returns the next completion by `(finish_time, unit_id)` and moves the clock there.

    The clock is the scheduler's too. Give it `step=timedelta(0)` so event timestamps do not
    move simulated time. Durations are whole seconds.
    """

    name: str = "sim"
    boundary: Literal["process", "task"] = "process"

    def __init__(
        self, durations: Mapping[str, int], failures: frozenset[str], clock: FakeClock
    ) -> None:
        self._durations = durations
        self._failures = failures
        self._clock = clock
        self._heap: list[tuple[float, str]] = []

    def submit(self, unit_id: str, *, queued_at: float) -> UnitHandle:
        heapq.heappush(self._heap, (self._clock.monotonic() + self._durations[unit_id], unit_id))
        return UnitHandle(unit_id)

    def wait_any(self) -> UnitCompletion:
        finish, unit_id = heapq.heappop(self._heap)
        self._clock.advance(max(0.0, finish - self._clock.monotonic()))
        return UnitCompletion(unit_id, "failed" if unit_id in self._failures else "succeeded")

    def cancel(self, handle: UnitHandle) -> bool:
        return False

    def shutdown(self, *, wait: bool) -> None:
        self._heap.clear()
