"""`StateStoreContract`: what every state store must do.

Subclass it in a test module, override the `store_factory` fixture, and set `subprocess_target`
to run the cross-process lock test::

    class TestMyStore(StateStoreContract):
        subprocess_target = ("my_pkg.store:MyStore", "state.db")

        @pytest.fixture
        def store_factory(self):
            return lambda tmp_path, clock: MyStore(tmp_path / "state.db", clock=clock)

`subprocess_target` is a `module:Class` that takes the store location as its only argument,
and that location relative to `tmp_path`.
"""

from __future__ import annotations

import subprocess
import sys
import threading
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

import pytest

from pipeloom.platform import Clock, SystemClock
from pipeloom.state.types import (
    LEASE_SECONDS,
    LockHeld,
    RunRecord,
    StateKey,
    StateStore,
    UnitRecord,
    Watermark,
)
from pipeloom.testing.clock import FakeClock

StoreFactory = Callable[[Path, Clock], StateStore]

KEY = StateKey("shop", "dev", "orders")
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

_CHILD = """
import importlib, sys
from pipeloom.state.types import LockHeld
module, _, name = sys.argv[1].partition(":")
store = getattr(importlib.import_module(module), name)(sys.argv[2])
try:
    with store.lock("run", "child"):
        pass
except LockHeld:
    sys.exit(3)
"""


def wm(value: Any, dtype: str = "int64", run_id: str = "r1") -> Watermark:
    return Watermark(value, dtype, run_id, T0)


class StateStoreContract:
    subprocess_target: ClassVar[tuple[str, str] | None] = None

    @pytest.fixture
    def store_factory(self) -> StoreFactory:
        raise NotImplementedError("override the store_factory fixture")

    @pytest.fixture
    def clock(self) -> FakeClock:
        return FakeClock(T0, step=timedelta(0))

    @pytest.fixture
    def store(self, store_factory: StoreFactory, tmp_path: Path, clock: FakeClock) -> StateStore:
        return store_factory(tmp_path, clock)

    def test_missing_watermark_is_none(self, store: StateStore) -> None:
        assert store.get_watermark(KEY) is None

    def test_cas_from_none_then_with_the_right_expected_value(self, store: StateStore) -> None:
        assert store.cas_watermark(KEY, None, wm(1))
        assert store.get_watermark(KEY) == wm(1)
        assert store.cas_watermark(KEY, wm(1), wm(2, run_id="r2"))
        assert store.get_watermark(KEY) == wm(2, run_id="r2")

    def test_cas_with_a_wrong_expected_value_changes_nothing(self, store: StateStore) -> None:
        assert not store.cas_watermark(KEY, wm(1), wm(2))
        assert store.get_watermark(KEY) is None
        assert store.cas_watermark(KEY, None, wm(1))
        assert not store.cas_watermark(KEY, None, wm(3))
        assert not store.cas_watermark(KEY, wm(9), wm(3))
        assert not store.cas_watermark(KEY, wm(1, dtype="string"), wm(3))
        assert store.get_watermark(KEY) == wm(1)

    def test_cas_succeeds_when_the_store_already_holds_new(self, store: StateStore) -> None:
        assert store.cas_watermark(KEY, None, wm(1, run_id="a"))
        assert store.cas_watermark(KEY, None, wm(1, run_id="a"))  # the same run replaying
        assert store.cas_watermark(KEY, wm(1), wm(1, run_id="a"))
        assert store.get_watermark(KEY) == wm(1, run_id="a")
        assert not store.cas_watermark(KEY, None, wm(2, run_id="a"))

    def test_cas_fails_when_another_run_stored_the_same_value(self, store: StateStore) -> None:
        assert store.cas_watermark(KEY, None, wm(1, run_id="a"))
        assert store.cas_watermark(KEY, wm(1), wm(5, run_id="a"))
        # run b read 1 too, computed 5 too, and lost the race
        assert not store.cas_watermark(KEY, wm(1), wm(5, run_id="b"))
        assert store.get_watermark(KEY) == wm(5, run_id="a")

    def test_cas_ignores_run_id_and_time_of_expected(self, store: StateStore) -> None:
        assert store.cas_watermark(KEY, None, wm(1, run_id="a"))
        assert store.cas_watermark(KEY, wm(1, run_id="other"), wm(2))

    def test_eight_threads_race_and_one_wins(self, store: StateStore) -> None:
        wins: list[bool] = []
        barrier = threading.Barrier(8)

        def race(i: int) -> None:
            barrier.wait()
            wins.append(store.cas_watermark(KEY, None, wm(i)))

        threads = [threading.Thread(target=race, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert wins.count(True) == 1 and len(wins) == 8
        assert store.get_watermark(KEY) is not None

    @pytest.mark.parametrize(
        ("value", "dtype"),
        [
            (datetime(2026, 3, 4, 5, 6, 7, 123456, tzinfo=UTC), "timestamp[UTC]"),
            (datetime(2026, 3, 4, 5, 6, 7), "timestamp"),
            (date(2026, 3, 4), "date"),
            (Decimal("12345.670"), "decimal(18,3)"),
            (42, "int64"),
            ("café", "string"),
        ],
    )
    def test_values_round_trip(self, store: StateStore, value: Any, dtype: str) -> None:
        assert store.cas_watermark(KEY, None, wm(value, dtype))
        got = store.get_watermark(KEY)
        assert got is not None and got.value == value and type(got.value) is type(value)
        assert got == wm(value, dtype)

    def test_run_upsert_and_unknown_run(self, store: StateStore) -> None:
        assert store.get_run("nope") is None
        rec = RunRecord(
            "r1", "shop", "dev", "hash", "running", T0, invocation_ids=("i1",), counts={"a": 1}
        )
        store.record_run(rec)
        assert store.get_run("r1") == rec
        done = RunRecord(
            "r1",
            "shop",
            "dev",
            "hash",
            "succeeded",
            T0,
            ended_at=T0 + timedelta(seconds=5),
            parent_run_id="r0",
            invocation_ids=("i1", "i2"),
            counts={"a": 2, "b": 3},
        )
        store.record_run(done)
        assert store.get_run("r1") == done

    def test_units_upsert_and_ordering(self, store: StateStore) -> None:
        def unit(uid: str, attempt: int, status: str = "running") -> UnitRecord:
            return UnitRecord(
                "r1", uid, attempt, status, "i1", started_at=T0, data={"rows": attempt, "k": [1]}
            )

        assert store.list_units("r1") == ()
        for u in (unit("model.b", 1), unit("model.a", 2), unit("model.a", 1)):
            store.record_unit(u)
        store.record_unit(unit("model.a", 1, "succeeded"))
        store.record_unit(UnitRecord("r2", "model.z", 1, "failed", "i9"))
        got = store.list_units("r1")
        assert [(u.unit_id, u.attempt) for u in got] == [
            ("model.a", 1),
            ("model.a", 2),
            ("model.b", 1),
        ]
        assert got[0] == unit("model.a", 1, "succeeded")
        assert [u.unit_id for u in store.list_units("r2")] == ["model.z"]

    def test_second_lock_raises_lock_held(self, store: StateStore) -> None:
        with store.lock("run", "r1") as info:
            assert info.scope == "run" and info.run_id == "r1"
            assert info.owner.count(":") >= 2
            assert info.expires_at - info.acquired_at == timedelta(seconds=LEASE_SECONDS)
            with pytest.raises(LockHeld) as err, store.lock("run", "r2"):
                pass
            assert err.value.info.owner == info.owner and err.value.info.run_id == "r1"
            with store.lock("other", "r3"):
                pass

    def test_lock_is_released_on_exception(self, store: StateStore) -> None:
        with pytest.raises(ZeroDivisionError), store.lock("run", "r1"):
            1 / 0  # noqa: B018
        with store.lock("run", "r2") as info:
            assert info.run_id == "r2"

    def test_expired_lease_is_taken_over(self, store: StateStore, clock: FakeClock) -> None:
        old = store.lock("run", "r1")
        first = old.__enter__()
        try:
            clock.advance(LEASE_SECONDS - 1)
            with pytest.raises(LockHeld), store.lock("run", "r2"):
                pass
            clock.advance(2)
            with store.lock("run", "r2") as taken:
                assert taken.owner != first.owner
        finally:
            old.__exit__(None, None, None)
        with store.lock("run", "r3"):
            pass

    def test_only_the_owner_releases(self, store: StateStore, clock: FakeClock) -> None:
        a = store.lock("run", "a")
        a.__enter__()
        clock.advance(LEASE_SECONDS + 1)
        b = store.lock("run", "b")
        b.__enter__()
        try:
            a.__exit__(None, None, None)  # A leaves while B holds the lock
            with pytest.raises(LockHeld) as err, store.lock("run", "c"):
                pass
            assert err.value.info.run_id == "b"
        finally:
            b.__exit__(None, None, None)
        with store.lock("run", "d"):
            pass

    def test_a_subprocess_cannot_take_a_held_lock(
        self, store_factory: StoreFactory, tmp_path: Path
    ) -> None:
        if self.subprocess_target is None:
            pytest.skip("subprocess_target is not set")
        ref, rel = self.subprocess_target
        store = store_factory(tmp_path, SystemClock())
        with store.lock("run", "parent"):
            proc = subprocess.run(
                [sys.executable, "-I", "-c", _CHILD, ref, str(tmp_path / rel)],
                capture_output=True,
                check=False,
                text=True,
                timeout=45,
            )
            assert proc.returncode == 3, proc.stderr
        ok = subprocess.run(
            [sys.executable, "-I", "-c", _CHILD, ref, str(tmp_path / rel)],
            capture_output=True,
            check=False,
            text=True,
            timeout=45,
        )
        assert ok.returncode == 0, ok.stderr
