"""State in one sqlite file. Local to one machine, so `shared=False`."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import AbstractContextManager, closing, contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

from pydantic import Field

from pipeloom.config.types import ProjectPath
from pipeloom.engine.types import StateStoreCapabilities
from pipeloom.platform import Clock, SystemClock
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec
from pipeloom.state import codec
from pipeloom.state.lease import Heartbeat, held, new_lock_info
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

SCHEMA_VERSION = 1
BUSY_TIMEOUT_MS = 30_000

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY, project TEXT NOT NULL, target TEXT NOT NULL,
    plan_hash TEXT NOT NULL, status TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT,
    parent_run_id TEXT, invocation_ids TEXT NOT NULL, counts TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS unit_results (
    run_id TEXT NOT NULL, unit_id TEXT NOT NULL, attempt INTEGER NOT NULL,
    status TEXT NOT NULL, invocation_id TEXT NOT NULL, started_at TEXT, ended_at TEXT,
    data TEXT NOT NULL, PRIMARY KEY (run_id, unit_id, attempt));
CREATE TABLE IF NOT EXISTS watermarks (
    project TEXT NOT NULL, target TEXT NOT NULL, model TEXT NOT NULL, value TEXT NOT NULL,
    dtype TEXT NOT NULL, run_id TEXT NOT NULL, updated_at TEXT NOT NULL,
    PRIMARY KEY (project, target, model));
CREATE TABLE IF NOT EXISTS locks (
    scope TEXT PRIMARY KEY, run_id TEXT NOT NULL, owner TEXT NOT NULL,
    acquired_at TEXT NOT NULL, expires_at TEXT NOT NULL);
"""


class SqliteStore:
    """One connection per call; writes run in `BEGIN IMMEDIATE`, so writers queue on the file."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Clock | None = None,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
        lease_seconds: float = LEASE_SECONDS,
    ) -> None:
        self._path = Path(path)
        self._clock: Clock = clock or SystemClock()
        self._heartbeat = heartbeat_seconds
        self.heartbeat = Heartbeat()
        """`heartbeat.last_error` is the last failed lease renewal, for the run to warn about."""
        self._lease = lease_seconds
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._write() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"{self._path} has state schema {version}; this pipeloom reads {SCHEMA_VERSION}"
                )
            for statement in _SCHEMA.split(";"):
                if statement.strip():
                    db.execute(statement)
            db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    @contextmanager
    def _read(self) -> Iterator[sqlite3.Connection]:
        with closing(sqlite3.connect(self._path, timeout=BUSY_TIMEOUT_MS / 1000)) as db:
            db.isolation_level = None
            db.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
            db.execute("PRAGMA journal_mode=WAL")
            yield db

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._read() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except BaseException:
                db.execute("ROLLBACK")
                raise
            db.execute("COMMIT")

    def get_watermark(self, key: StateKey) -> Watermark | None:
        with self._read() as db:
            row = db.execute(
                "SELECT value, dtype, run_id, updated_at FROM watermarks "
                "WHERE project=? AND target=? AND model=?",
                (key.project, key.target, key.model),
            ).fetchone()
        return None if row is None else _watermark(row)

    def cas_watermark(self, key: StateKey, expected: Watermark | None, new: Watermark) -> bool:
        with self._write() as db:
            row = db.execute(
                "SELECT value, dtype, run_id, updated_at FROM watermarks "
                "WHERE project=? AND target=? AND model=?",
                (key.project, key.target, key.model),
            ).fetchone()
            current = None if row is None else _watermark(row)
            if (current is None) != (expected is None) or (
                current is not None
                and expected is not None
                and codec.value_key(current) != codec.value_key(expected)
            ):
                return False
            db.execute(
                "INSERT OR REPLACE INTO watermarks VALUES (?,?,?,?,?,?,?)",
                (
                    key.project,
                    key.target,
                    key.model,
                    codec.dumps(codec.encode_value(new.value)),
                    new.dtype,
                    new.run_id,
                    new.updated_at.isoformat(),
                ),
            )
        return True

    def record_run(self, record: RunRecord) -> None:
        with self._write() as db:
            db.execute(
                "INSERT OR REPLACE INTO runs VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    record.run_id,
                    record.project,
                    record.target,
                    record.plan_hash,
                    record.status,
                    record.started_at.isoformat(),
                    codec.dt_text(record.ended_at),
                    record.parent_run_id,
                    codec.dumps(list(record.invocation_ids)),
                    codec.dumps(dict(record.counts)),
                ),
            )

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._read() as db:
            row = db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        return RunRecord(
            run_id=row[0],
            project=row[1],
            target=row[2],
            plan_hash=row[3],
            status=row[4],
            started_at=codec.dt_load(row[5]),  # type: ignore[arg-type]
            ended_at=codec.dt_load(row[6]),
            parent_run_id=row[7],
            invocation_ids=tuple(_loads(row[8])),
            counts=_loads(row[9]),
        )

    def record_unit(self, record: UnitRecord) -> None:
        with self._write() as db:
            db.execute(
                "INSERT OR REPLACE INTO unit_results VALUES (?,?,?,?,?,?,?,?)",
                (
                    record.run_id,
                    record.unit_id,
                    record.attempt,
                    record.status,
                    record.invocation_id,
                    codec.dt_text(record.started_at),
                    codec.dt_text(record.ended_at),
                    codec.dumps(dict(record.data)),
                ),
            )

    def list_units(self, run_id: str) -> tuple[UnitRecord, ...]:
        with self._read() as db:
            rows = db.execute(
                "SELECT * FROM unit_results WHERE run_id=? ORDER BY unit_id, attempt", (run_id,)
            ).fetchall()
        # SQLite orders text by bytes; sort again by Python string to match the json store.
        units = [
            UnitRecord(
                run_id=r[0],
                unit_id=r[1],
                attempt=r[2],
                status=r[3],
                invocation_id=r[4],
                started_at=codec.dt_load(r[5]),
                ended_at=codec.dt_load(r[6]),
                data=_loads(r[7]),
            )
            for r in rows
        ]
        return tuple(sorted(units, key=lambda u: (u.unit_id, u.attempt)))

    def lock(self, scope: str, run_id: str) -> AbstractContextManager[LockInfo]:
        info = new_lock_info(scope, run_id, self._clock, self._lease)
        with self._write() as db:
            row = db.execute(
                "SELECT scope, run_id, owner, acquired_at, expires_at FROM locks WHERE scope=?",
                (scope,),
            ).fetchone()
            if row is not None:
                existing = LockInfo(row[0], row[1], row[2], *map(_dt, row[3:5]))
                if existing.expires_at > self._clock.now():
                    raise LockHeld(existing)
            db.execute(
                "INSERT OR REPLACE INTO locks VALUES (?,?,?,?,?)",
                (
                    scope,
                    run_id,
                    info.owner,
                    info.acquired_at.isoformat(),
                    info.expires_at.isoformat(),
                ),
            )
        return held(info, self._renew, self._release, self._heartbeat, self.heartbeat)

    def _renew(self, info: LockInfo) -> None:
        expires = self._clock.now() + timedelta(seconds=self._lease)
        with self._write() as db:
            db.execute(
                "UPDATE locks SET expires_at=? WHERE scope=? AND owner=?",
                (expires.isoformat(), info.scope, info.owner),
            )

    def _release(self, info: LockInfo) -> None:
        with self._write() as db:
            db.execute("DELETE FROM locks WHERE scope=? AND owner=?", (info.scope, info.owner))

    def close(self) -> None:
        """Nothing to close: every call opens and closes its own connection."""


def _dt(text: str) -> Any:
    return codec.dt_load(text)


def _loads(text: str) -> Any:
    return json.loads(text)


def _watermark(row: tuple[str, str, str, str]) -> Watermark:
    value, dtype, run_id, updated_at = row
    return Watermark(codec.decode_value(dtype, _loads(value)), dtype, run_id, _dt(updated_at))


class SqliteOptions(OptionsModel):
    path: ProjectPath = Field(".pipeloom/state.db", validate_default=True)
    """Database file, relative to the project directory."""


def _build(opts: SqliteOptions) -> StateStore:
    return SqliteStore(opts.path)


SPEC: PluginSpec[StateStore] = PluginSpec(
    "sqlite",
    "sqlite",
    "state_stores",
    API_VERSION,
    _build,
    SqliteOptions,
    capabilities=StateStoreCapabilities(shared=False),
    description="State in one sqlite file on this machine.",
)
