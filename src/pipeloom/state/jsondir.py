"""State as JSON files in a directory. Fit for a shared volume, so `shared=True`.

Every write goes to `<name>.<uuid>.tmp` and is moved into place with `os.replace`. Locks are
lease files created with `O_EXCL`; an expired one is taken over by renaming it away first, which
only one contender can do for a given file. Known race: a contender that read an expired lease
can rename away a fresh lease that a faster contender created in between. It checks the owner
after the rename and puts the lease back, but between the rename and the restore the scope looks
free and a third process can take it, and on a file system without hard links or with a
contender already in place the restore can fail. ponytail: `O_EXCL` and `os.replace` are weak on NFS and SMB; a
database-backed store is the answer there.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, suppress
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, TypeVar
from urllib.parse import quote

from pydantic import Field

from pipeloom.config.types import ProjectPath
from pipeloom.engine.types import StateStoreCapabilities
from pipeloom.platform import Clock, SystemClock
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.state import codec
from pipeloom.state.lease import Heartbeat, expired, held, new_lock_info
from pipeloom.state.types import (
    HEARTBEAT_SECONDS,
    LEASE_SECONDS,
    LockHeld,
    LockInfo,
    RunRecord,
    StateKey,
    StateStore,
    UnitRecord,
    Watermark,
)

SCHEMA = "pipeloom.state/1"
MUTEX_LEASE_SECONDS = 30
REPLACE_RETRIES = 10
REPLACE_BACKOFF = 0.05

T = TypeVar("T")


def _retry(fn: Callable[[], T]) -> T:
    """Windows refuses to replace or open a file another process has open; try again."""
    for attempt in range(REPLACE_RETRIES):
        try:
            return fn()
        except PermissionError:
            if sys.platform != "win32" or attempt == REPLACE_RETRIES - 1:
                raise
            time.sleep(REPLACE_BACKOFF)
    raise AssertionError("unreachable")


def _seg(text: str) -> str:
    """One safe path segment: percent-encoded, never `.`, `..` or ending in a dot."""
    q = quote(text, safe="")
    if q in ("", ".", "..") or q.endswith("."):
        q = q[:-1] + "%2E" if q else "%00"
    return q


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_bytes((json.dumps(obj, sort_keys=True, indent=2, ensure_ascii=True) + "\n").encode())
    try:
        _retry(lambda: os.replace(tmp, path))
    except BaseException:
        with suppress(OSError):
            tmp.unlink()
        raise


def _read_json(path: Path) -> Any | None:
    try:
        return json.loads(_retry(path.read_bytes).decode("utf-8"))
    except FileNotFoundError:
        return None


def _read_lock(path: Path) -> LockInfo | None:
    """The lease in a lock file; None when the file is gone. A half-written file is retried."""
    for _ in range(5):
        try:
            data = _read_json(path)
        except ValueError:
            data = None
        else:
            if data is None:
                return None
        if data is not None:
            return codec.lock_from_json(data)
        time.sleep(0.02)
    # Created but never filled (the writer died): age the lease from the file's mtime.
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    except FileNotFoundError:
        return None
    return LockInfo(path.stem, "", "unknown", mtime, mtime + timedelta(seconds=LEASE_SECONDS))


class JsonDirStore:
    def __init__(
        self,
        path: str | Path,
        *,
        clock: Clock | None = None,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
        lease_seconds: float = LEASE_SECONDS,
    ) -> None:
        self._root = Path(path)
        self._clock: Clock = clock or SystemClock()
        self._heartbeat = heartbeat_seconds
        self.heartbeat = Heartbeat()
        """`heartbeat.last_error` is the last failed lease renewal, for the run to warn about."""
        self._lease = lease_seconds
        marker = self._root / "_state.json"
        self._root.mkdir(parents=True, exist_ok=True)
        found = _read_json(marker)
        if found is None:
            _write_json(marker, {"schema": SCHEMA})
        elif found.get("schema") != SCHEMA:
            raise RuntimeError(f"{marker} is not a {SCHEMA} state directory")

    # -- watermarks --------------------------------------------------------------------------

    def _wm_path(self, key: StateKey) -> Path:
        return (
            self._root
            / "watermarks"
            / _seg(key.project)
            / _seg(key.target)
            / (_seg(key.model) + ".json")
        )

    def get_watermark(self, key: StateKey) -> Watermark | None:
        data = _read_json(self._wm_path(key))
        return None if data is None else codec.watermark_from_json(data)

    def cas_watermark(self, key: StateKey, expected: Watermark | None, new: Watermark) -> bool:
        name = f"wm-{_seg(key.project)}-{_seg(key.target)}-{_seg(key.model)}.mutex"
        with self._mutex(name):
            current = self.get_watermark(key)
            if (current is None) != (expected is None):
                return False
            if (
                current is not None
                and expected is not None
                and codec.value_key(current) != codec.value_key(expected)
            ):
                return False
            _write_json(self._wm_path(key), codec.watermark_to_json(new))
            return True

    # -- runs and units ----------------------------------------------------------------------

    def _run_dir(self, run_id: str) -> Path:
        return self._root / "runs" / _seg(run_id)

    def record_run(self, record: RunRecord) -> None:
        _write_json(self._run_dir(record.run_id) / "run.json", codec.run_to_json(record))

    def get_run(self, run_id: str) -> RunRecord | None:
        data = _read_json(self._run_dir(run_id) / "run.json")
        return None if data is None else codec.run_from_json(data)

    def record_unit(self, record: UnitRecord) -> None:
        path = (
            self._run_dir(record.run_id)
            / "units"
            / _seg(record.unit_id)
            / f"attempt-{record.attempt}.json"
        )
        _write_json(path, codec.unit_to_json(record))

    def list_units(self, run_id: str) -> tuple[UnitRecord, ...]:
        units: list[UnitRecord] = []
        for file in (self._run_dir(run_id) / "units").glob("*/attempt-*.json"):
            data = _read_json(file)
            if data is not None:
                units.append(codec.unit_from_json(data))
        return tuple(sorted(units, key=lambda u: (u.unit_id, u.attempt)))

    # -- locks -------------------------------------------------------------------------------

    def _lock_path(self, name: str) -> Path:
        return self._root / "locks" / name

    def _acquire(self, path: Path, info: LockInfo) -> None:
        """Create the lease file for `info`, or raise `LockHeld`."""
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = (json.dumps(codec.lock_to_json(info), sort_keys=True) + "\n").encode()
        while True:
            try:
                fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                pass
            else:
                with os.fdopen(fd, "wb") as fh:
                    fh.write(payload)
                return
            existing = _read_lock(path)
            if existing is None:
                continue  # released between our create and our read
            if not expired(existing, self._clock):
                raise LockHeld(existing)
            stale = path.with_name(f"{path.name}.stale-{uuid.uuid4().hex}")
            try:
                _retry(partial(os.rename, path, stale))
            except FileNotFoundError:
                continue  # another contender took it over first
            moved = _read_lock(stale)
            if moved is not None and moved.owner != existing.owner:
                # Raced: the lease we renamed was renewed or retaken after we read it. Put it
                # back (a hard link fails if a contender already created the file, or on file
                # systems without links; then try a rename) and never delete it.
                try:
                    os.link(stale, path)
                except OSError:
                    with suppress(OSError):
                        os.rename(stale, path)
                else:
                    with suppress(OSError):
                        stale.unlink()
                raise LockHeld(moved)
            with suppress(OSError):
                stale.unlink()

    def _renew_file(self, path: Path, lease: float) -> Callable[[LockInfo], None]:
        def renew(info: LockInfo) -> None:
            current = _read_lock(path)
            if current is None or current.owner != info.owner:
                return
            now = self._clock.now()
            _write_json(
                path,
                codec.lock_to_json(
                    LockInfo(
                        info.scope,
                        info.run_id,
                        info.owner,
                        info.acquired_at,
                        now + timedelta(seconds=lease),
                    )
                ),
            )

        return renew

    def _release_file(self, path: Path) -> Callable[[LockInfo], None]:
        def release(info: LockInfo) -> None:
            current = _read_lock(path)
            if current is not None and current.owner == info.owner:
                with suppress(FileNotFoundError):
                    _retry(path.unlink)

        return release

    def lock(self, scope: str, run_id: str) -> AbstractContextManager[LockInfo]:
        path = self._lock_path(quote(scope, safe="") + ".lock")
        info = new_lock_info(scope, run_id, self._clock, self._lease)
        self._acquire(path, info)
        return held(
            info,
            self._renew_file(path, self._lease),
            self._release_file(path),
            self._heartbeat,
            self.heartbeat,
        )

    @contextmanager
    def _mutex(self, name: str) -> Iterator[None]:
        """Wait for the short lease that serialises one watermark's read-compare-write."""
        path = self._lock_path(name)
        info = new_lock_info(name, "", self._clock, MUTEX_LEASE_SECONDS)
        while True:
            try:
                self._acquire(path, info)
                break
            except LockHeld:
                time.sleep(0.01)
        try:
            yield
        finally:
            self._release_file(path)(info)

    def close(self) -> None:
        """Nothing to close."""


class JsonOptions(OptionsModel):
    path: ProjectPath = Field(".pipeloom/state", validate_default=True)
    """State directory, relative to the project directory."""


def _build(opts: JsonOptions) -> StateStore:
    return JsonDirStore(opts.path)


SPEC: PluginSpec[StateStore] = PluginSpec(
    "json",
    "json",
    "state_stores",
    API_VERSION,
    _build,
    JsonOptions,
    capabilities=StateStoreCapabilities(shared=True),
    description="State as JSON files in a directory; safe to share between processes.",
)
