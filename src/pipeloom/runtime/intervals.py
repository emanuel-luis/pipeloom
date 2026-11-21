"""Interval incrementals (ADR-0054): windows, interval ids and the interval ledger.

Windows are half-open UTC ranges `[start, end)` of one grain. The first window starts at `begin`
truncated down to the grain; the last is the window that holds the run's start time, so the
current, partial window runs too. The ledger records the time each model has processed as a
merged list of `[start, end]` ISO pairs, under a reserved state key per model and target.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, NamedTuple

from pipeloom.platform import Clock
from pipeloom.runtime.incremental import family
from pipeloom.state.types import StateKey, StateStore, Watermark

Grain = Literal["hour", "day", "week", "month", "year"]

LEDGER_TYPE: Final = "pipeloom.intervals/1"
"""The document type of a ledger, stored as the dtype of its state record."""
LEDGER_SUFFIX: Final = "@intervals"
"""Model names are identifiers, so `<model>@intervals` never names a model's watermark."""
_CAS_ATTEMPTS: Final = 50


class IntervalError(Exception):
    """An interval model cannot run as configured (PLM209)."""


class Window(NamedTuple):
    start: datetime
    end: datetime

    @property
    def id(self) -> str:
        """Deterministic: the same bounds give the same id in every run."""
        return f"{self.start:%Y%m%dT%H%M%SZ}_{self.end:%Y%m%dT%H%M%SZ}"


def to_utc(value: datetime) -> datetime:
    """A naive datetime is read as UTC; an aware one is converted."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def truncate(value: datetime, grain: Grain) -> datetime:
    """The start of the window that holds `value` (weeks start on Monday)."""
    v = to_utc(value)
    if grain == "hour":
        return v.replace(minute=0, second=0, microsecond=0)
    day = v.replace(hour=0, minute=0, second=0, microsecond=0)
    if grain == "day":
        return day
    if grain == "week":
        return day - timedelta(days=day.weekday())
    if grain == "month":
        return day.replace(day=1)
    return day.replace(month=1, day=1)


def advance(start: datetime, grain: Grain) -> datetime:
    """The start of the next window; `start` is a window start."""
    if grain == "hour":
        return start + timedelta(hours=1)
    if grain == "day":
        return start + timedelta(days=1)
    if grain == "week":
        return start + timedelta(weeks=1)
    if grain == "month":
        y, m = divmod(start.month, 12)
        return start.replace(year=start.year + y, month=m + 1)
    return start.replace(year=start.year + 1)


def windows(begin: datetime, end: datetime, grain: Grain) -> list[Window]:
    """Every window that overlaps `[begin, end)`, first to last."""
    out: list[Window] = []
    start, stop = truncate(begin, grain), to_utc(end)
    if to_utc(begin) >= stop:
        return out
    while start < stop:
        nxt = advance(start, grain)
        out.append(Window(start, nxt))
        start = nxt
    return out


def merge(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    """Sorted, with overlapping and touching intervals joined; empty ones dropped."""
    out: list[tuple[datetime, datetime]] = []
    for s, e in sorted(i for i in intervals if i[0] < i[1]):
        if out and s <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], e))
        else:
            out.append((s, e))
    return out


def _covered(start: datetime, end: datetime, ledger: list[tuple[datetime, datetime]]) -> bool:
    return any(s <= start and end <= e for s, e in ledger)


def to_run(
    begin: datetime,
    now: datetime,
    grain: Grain,
    ledger: list[tuple[datetime, datetime]],
    lookback: int,
    *,
    full_refresh: bool = False,
) -> list[Window]:
    """The windows a run processes, first to last.

    Without a ledger (or on a full refresh): every window from `begin` to the one holding `now`.
    Otherwise the windows whose time up to `now` the ledger does not cover, plus the `lookback`
    windows just before the first of them. Nothing is missing: nothing runs.
    """
    now = to_utc(now)
    every = windows(begin, now, grain)
    if full_refresh or not ledger:
        return every
    todo = [i for i, w in enumerate(every) if not _covered(w.start, min(w.end, now), ledger)]
    if not todo:
        return []
    first = max(0, todo[0] - lookback)
    keep = set(todo) | set(range(first, todo[0]))
    return [w for i, w in enumerate(every) if i in keep]


def bounds(dtype: str, column: str, window: Window, grain: Grain) -> tuple[Any, Any]:
    """The window bounds as values of the event_time column's type."""
    fam = family(dtype)
    if fam == "timestamp_tz":
        return window.start, window.end
    if fam == "timestamp":  # naive values are UTC
        return window.start.replace(tzinfo=None), window.end.replace(tzinfo=None)
    if fam == "date":
        if grain == "hour":
            raise IntervalError(f"grain hour needs a timestamp column, but '{column}' is {dtype}")
        return window.start.date(), window.end.date()
    raise IntervalError(f"event_time '{column}' is {dtype}; it needs a timestamp or date column")


# -- the ledger --------------------------------------------------------------------------------


def ledger_key(key: StateKey) -> StateKey:
    """The reserved key of a model's ledger. The target is part of it: a dev target never
    reads or writes the ledger of prod."""
    return StateKey(key.project, key.target, key.model + LEDGER_SUFFIX)


def _decode(record: Watermark | None) -> list[tuple[datetime, datetime]]:
    if record is None:
        return []
    if record.dtype != LEDGER_TYPE or not isinstance(record.value, list):
        raise IntervalError(f"the interval ledger holds {record.dtype}, not {LEDGER_TYPE}")
    return [(datetime.fromisoformat(s), datetime.fromisoformat(e)) for s, e in record.value]


def _encode(intervals: list[tuple[datetime, datetime]]) -> list[list[str]]:
    """ISO text: the state codec does not convert datetimes nested in a list."""
    return [[s.isoformat(), e.isoformat()] for s, e in intervals]


def load(state: StateStore, key: StateKey) -> list[tuple[datetime, datetime]]:
    return _decode(state.get_watermark(ledger_key(key)))


def record(
    state: StateStore,
    key: StateKey,
    start: datetime,
    end: datetime,
    *,
    run_id: str,
    clock: Clock,
) -> None:
    """Merge `[start, end)` into the ledger. A compare-and-swap loop: a concurrent writer makes
    this one read again and merge again, so no interval is lost."""
    k = ledger_key(key)
    for _ in range(_CAS_ATTEMPTS):
        current = state.get_watermark(k)
        have = _decode(current)
        merged = merge([*have, (to_utc(start), to_utc(end))])
        if current is not None and merged == have:
            return
        new = Watermark(_encode(merged), LEDGER_TYPE, run_id, clock.now())
        if state.cas_watermark(k, current, new):
            return
    raise IntervalError(f"the interval ledger of '{key.model}' kept changing; run again")


__all__ = [
    "LEDGER_TYPE",
    "Grain",
    "IntervalError",
    "Window",
    "advance",
    "bounds",
    "ledger_key",
    "load",
    "merge",
    "record",
    "to_run",
    "to_utc",
    "truncate",
    "windows",
]
