"""A clock tests control."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta


class FakeClock:
    """`now()` returns the current time, then moves it forward by `step`.

    `monotonic()` is the seconds elapsed since `start` and does not move the clock.
    """

    def __init__(
        self, start: datetime | None = None, step: timedelta = timedelta(seconds=1)
    ) -> None:
        self._start = start or datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        if self._start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        self._now = self._start
        self.step = step

    def now(self) -> datetime:
        value = self._now
        self._now += self.step
        return value

    def monotonic(self) -> float:
        return (self._now - self._start).total_seconds()

    def advance(self, delta: timedelta | float) -> None:
        self._now += delta if isinstance(delta, timedelta) else timedelta(seconds=delta)
