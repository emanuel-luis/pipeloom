"""Window math and the interval ledger (ADR-0054)."""

from __future__ import annotations

import itertools
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pipeloom.runtime import intervals
from pipeloom.runtime.intervals import IntervalError, Window
from pipeloom.state.sqlite import SqliteStore
from pipeloom.state.types import StateKey, Watermark
from pipeloom.testing.clock import FakeClock

GRAINS = ("hour", "day", "week", "month", "year")
moments = st.datetimes(
    min_value=datetime(1990, 1, 1), max_value=datetime(2090, 1, 1), timezones=st.just(UTC)
)
grains = st.sampled_from(GRAINS)


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


@given(moments, grains)
def test_truncate_holds_the_value_and_is_idempotent(value: datetime, grain: str) -> None:
    start = intervals.truncate(value, grain)  # type: ignore[arg-type]
    end = intervals.advance(start, grain)  # type: ignore[arg-type]
    assert start <= value < end
    assert intervals.truncate(start, grain) == start  # type: ignore[arg-type]
    assert start.tzinfo is UTC


@given(moments, grains)
def test_the_boundary_and_one_past_it(value: datetime, grain: str) -> None:
    start = intervals.truncate(value, grain)  # type: ignore[arg-type]
    end = intervals.advance(start, grain)  # type: ignore[arg-type]
    assert intervals.truncate(end, grain) == end  # type: ignore[arg-type]
    assert intervals.truncate(end - timedelta(microseconds=1), grain) == start  # type: ignore[arg-type]
    assert intervals.truncate(end + timedelta(microseconds=1), grain) == end  # type: ignore[arg-type]


@given(moments, st.timedeltas(min_value=timedelta(0), max_value=timedelta(days=800)), grains)
def test_windows_are_contiguous_and_cover_the_range(
    begin: datetime, span: timedelta, grain: str
) -> None:
    end = begin + span
    ws = intervals.windows(begin, end, grain)  # type: ignore[arg-type]
    if begin == end:
        assert ws == []
        return
    assert ws[0].start <= begin < ws[0].end
    assert ws[-1].start < end <= ws[-1].end
    assert all(x.end == y.start for x, y in itertools.pairwise(ws))
    assert len({w.id for w in ws}) == len(ws)


def test_month_and_year_truncation() -> None:
    assert intervals.truncate(utc(2024, 2, 29, 23, 59), "month") == utc(2024, 2, 1)
    assert intervals.advance(utc(2024, 12, 1), "month") == utc(2025, 1, 1)
    assert intervals.advance(utc(2024, 1, 1), "month") == utc(2024, 2, 1)
    assert intervals.truncate(utc(2024, 12, 31, 23), "year") == utc(2024, 1, 1)
    assert intervals.advance(utc(2024, 1, 1), "year") == utc(2025, 1, 1)
    assert intervals.truncate(utc(2024, 1, 3), "week") == utc(2024, 1, 1)  # a Monday
    ws = intervals.windows(utc(2024, 1, 15), utc(2024, 4, 1), "month")
    assert [w.start.month for w in ws] == [1, 2, 3]


def test_a_dst_change_does_not_move_utc_windows() -> None:
    zone = ZoneInfo("America/New_York")  # tzdata on Windows
    before = datetime(2024, 3, 9, 12, tzinfo=zone)  # EST, UTC-5
    after = datetime(2024, 3, 11, 12, tzinfo=zone)  # EDT, UTC-4
    ws = intervals.windows(before, after, "day")
    assert [w.start for w in ws] == [utc(2024, 3, 9), utc(2024, 3, 10), utc(2024, 3, 11)]
    assert all(w.end - w.start == timedelta(days=1) for w in ws)
    hours = intervals.windows(
        datetime(2024, 3, 10, 1, tzinfo=zone), datetime(2024, 3, 10, 4, tzinfo=zone), "hour"
    )
    assert len(hours) == 2  # 01:00 to 04:00 local is two real hours on the spring-forward day
    assert intervals.to_utc(datetime(2024, 1, 1)) == utc(2024, 1, 1)  # naive means UTC


def test_merge_joins_touching_and_overlapping() -> None:
    a, b, c, d = (utc(2024, 1, n) for n in (1, 2, 3, 5))
    assert intervals.merge([(b, c), (a, b), (d, d)]) == [(a, c)]
    assert intervals.merge([(a, c), (b, c), (d, utc(2024, 1, 6))]) == [(a, c), (d, utc(2024, 1, 6))]


def test_to_run_without_a_ledger_runs_every_window_up_to_now() -> None:
    now = utc(2024, 1, 3, 6)
    ws = intervals.to_run(utc(2024, 1, 1), now, "day", [], 0)
    assert [w.start.day for w in ws] == [1, 2, 3]


def test_to_run_runs_only_the_gap_and_the_partial_window() -> None:
    now = utc(2024, 1, 6, 6)
    ledger = [(utc(2024, 1, 1), utc(2024, 1, 3)), (utc(2024, 1, 4), utc(2024, 1, 5, 12))]
    ws = intervals.to_run(utc(2024, 1, 1), now, "day", ledger, 0)
    assert [w.start.day for w in ws] == [3, 5, 6]
    again = intervals.to_run(utc(2024, 1, 1), now, "day", [(utc(2024, 1, 1), now)], 0)
    assert again == []
    looked = intervals.to_run(utc(2024, 1, 1), now, "day", ledger, 1)
    assert [w.start.day for w in looked] == [2, 3, 5, 6]
    refresh = intervals.to_run(utc(2024, 1, 1), now, "day", ledger, 0, full_refresh=True)
    assert len(refresh) == 6


def test_bounds_follow_the_column_type() -> None:
    w = Window(utc(2024, 1, 1), utc(2024, 1, 2))
    assert intervals.bounds("timestamp[us, UTC]", "ts", w, "day") == (w.start, w.end)
    naive = intervals.bounds("timestamp[us]", "ts", w, "day")
    assert naive[0].tzinfo is None and naive[0] == datetime(2024, 1, 1)
    assert intervals.bounds("date", "d", w, "day") == (w.start.date(), w.end.date())
    with pytest.raises(IntervalError, match="hour"):
        intervals.bounds("date", "d", w, "hour")
    with pytest.raises(IntervalError, match="int64"):
        intervals.bounds("int64", "n", w, "day")


def test_the_ledger_key_is_per_target_and_cannot_name_a_model() -> None:
    prod = intervals.ledger_key(StateKey("p", "prod", "m"))
    dev = intervals.ledger_key(StateKey("p", "dev", "m"))
    assert prod != dev and prod.model == "m@intervals"


def test_a_foreign_record_under_the_ledger_key_is_an_error(tmp_path: object) -> None:
    store = SqliteStore(f"{tmp_path}/s.db")
    key = StateKey("p", "t", "m")
    store.cas_watermark(
        intervals.ledger_key(key), None, Watermark(1, "int64", "r", utc(2024, 1, 1))
    )
    with pytest.raises(IntervalError, match="int64"):
        intervals.load(store, key)
    store.close()


def test_record_merges_and_skips_a_known_interval(tmp_path: object) -> None:
    store = SqliteStore(f"{tmp_path}/s.db")
    key, clock = StateKey("p", "t", "m"), FakeClock()
    intervals.record(store, key, utc(2024, 1, 2), utc(2024, 1, 3), run_id="r1", clock=clock)
    intervals.record(store, key, utc(2024, 1, 1), utc(2024, 1, 2), run_id="r2", clock=clock)
    intervals.record(store, key, utc(2024, 1, 1), utc(2024, 1, 2), run_id="r3", clock=clock)
    assert intervals.load(store, key) == [(utc(2024, 1, 1), utc(2024, 1, 3))]
    raw = store.get_watermark(intervals.ledger_key(key))
    assert raw is not None and raw.dtype == "pipeloom.intervals/1" and raw.run_id == "r2"
    store.close()
