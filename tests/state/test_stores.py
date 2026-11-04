from __future__ import annotations

import os
import threading
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeloom.platform import Clock
from pipeloom.state import jsondir
from pipeloom.state.jsondir import JsonDirStore, _seg
from pipeloom.state.lease import Heartbeat, held, new_lock_info, new_owner
from pipeloom.state.sqlite import SqliteStore
from pipeloom.state.types import LockHeld, StateStore
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.contract.state import KEY, T0, StateStoreContract, StoreFactory, wm


class TestSqliteContract(StateStoreContract):
    subprocess_target = ("pipeloom.state.sqlite:SqliteStore", "state.db")

    @pytest.fixture
    def store_factory(self) -> StoreFactory:
        return lambda tmp, clock: SqliteStore(tmp / "state.db", clock=clock)


class TestJsonContract(StateStoreContract):
    subprocess_target = ("pipeloom.state.jsondir:JsonDirStore", "state")

    @pytest.fixture
    def store_factory(self) -> StoreFactory:
        return lambda tmp, clock: JsonDirStore(tmp / "state", clock=clock)


def test_owner_format() -> None:
    host, pid, tail = new_owner().rsplit(":", 2)
    assert host and pid == str(os.getpid()) and len(tail) == 8


def test_heartbeat_renews_and_stops(tmp_path: Path) -> None:
    clock = FakeClock(step=timedelta(0))
    info = new_lock_info("s", "r", clock)
    beats = threading.Event()
    released: list[str] = []

    with held(info, lambda _i: beats.set(), lambda i: released.append(i.owner), 0.01):
        assert beats.wait(5)
    assert released == [info.owner]


def test_sqlite_schema_version(tmp_path: Path) -> None:
    import sqlite3

    SqliteStore(tmp_path / "s.db")
    db = sqlite3.connect(tmp_path / "s.db")
    try:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        db.execute("PRAGMA user_version=2")
        db.commit()
    finally:
        db.close()
    with pytest.raises(RuntimeError, match="schema 2"):
        SqliteStore(tmp_path / "s.db")


def test_json_layout_and_marker(tmp_path: Path) -> None:
    from pipeloom.state.types import RunRecord, UnitRecord

    store = JsonDirStore(tmp_path / "st")
    store.record_run(RunRecord("r/1", "p", "t", "h", "created", T0))
    store.record_unit(UnitRecord("r/1", "model.p.a", 1, "ok", "i"))
    assert (tmp_path / "st" / "_state.json").read_text(encoding="utf-8").count("pipeloom.state/1")
    assert (tmp_path / "st" / "runs" / "r%2F1" / "run.json").is_file()
    assert (tmp_path / "st" / "runs" / "r%2F1" / "units" / "model.p.a" / "attempt-1.json").is_file()
    assert not list((tmp_path / "st").rglob("*.tmp"))
    (tmp_path / "st" / "_state.json").write_text('{"schema": "x"}', encoding="utf-8")
    with pytest.raises(RuntimeError):
        JsonDirStore(tmp_path / "st")


@pytest.mark.parametrize("raw", ["..", ".", "", "a/b", "x."])
def test_segments_cannot_escape(raw: str) -> None:
    seg = _seg(raw)
    assert "/" not in seg and seg not in ("", ".", "..") and not seg.endswith(".")


def test_json_lock_file_and_stale_takeover(tmp_path: Path) -> None:
    clock: Clock = FakeClock(step=timedelta(0))
    store = JsonDirStore(tmp_path, clock=clock)
    cm = store.lock("my scope", "r1")
    file = tmp_path / "locks" / "my%20scope.lock"
    assert file.is_file()
    with cm:
        pass
    assert not file.exists()
    # A corrupt (empty) lock file ages from its mtime instead of blocking forever.
    file.write_bytes(b"")
    os.utime(file, (1, 1))
    with store.lock("my scope", "r2"):
        pass
    assert not list((tmp_path / "locks").glob("*.stale-*"))


def test_stores_satisfy_the_protocol(tmp_path: Path) -> None:
    assert isinstance(SqliteStore(tmp_path / "a.db"), StateStore)
    assert isinstance(JsonDirStore(tmp_path / "b"), StateStore)


def test_lock_held_message(tmp_path: Path) -> None:
    store = SqliteStore(tmp_path / "a.db")
    with store.lock("s", "r1"), pytest.raises(LockHeld, match="held by"):
        store.lock("s", "r2")


def test_heartbeat_survives_a_failed_renewal(tmp_path: Path) -> None:
    info = new_lock_info("s", "r", FakeClock(step=timedelta(0)))
    calls: list[int] = []
    renewed = threading.Event()
    status = Heartbeat()

    def renew(_i: object) -> None:
        calls.append(1)
        if len(calls) == 1:
            raise OSError("transient")
        renewed.set()

    with held(info, renew, lambda _i: None, 0.01, status):
        assert renewed.wait(5)
    assert isinstance(status.last_error, OSError)


def test_a_delete_pending_lease_file_is_retried_on_windows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows raises PermissionError from O_EXCL while a releasing contender's unlink is pending."""
    store = JsonDirStore(tmp_path / "state")
    real_open, denied = os.open, []

    def flaky(path: str | Path, flags: int, *args: int) -> int:
        if str(path).endswith(".mutex") and not denied:
            denied.append(path)
            raise PermissionError(13, "Permission denied")
        return real_open(path, flags, *args)

    monkeypatch.setattr(jsondir.os, "open", flaky)
    monkeypatch.setattr(jsondir, "sys", SimpleNamespace(platform="win32"))
    assert store.cas_watermark(KEY, None, wm(1))
    assert denied
