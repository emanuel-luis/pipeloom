"""The `table` state store: watermarks, runs, units and locks in one Delta table.

The table is partitioned by `kind`; a row is found by (kind, key), where `key` is the canonical
JSON array of the record's natural key. `data` holds the record in the codec's JSON, so it reads
back exactly; the other columns are for queries and for the MERGE conditions.

Every insert or replace is one `MERGE` whose source is a one-row DataFrame, so no value is
spliced into SQL text; the MERGE conditions make compare-and-set and lease taking atomic, and a
Delta conflict between concurrent writers is retried. The store reuses the Spark session it
finds and builds one from `session` only when there is none; it stops only what it built.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, ExitStack
from datetime import UTC, datetime, timedelta
from typing import Any, Final, TypeVar

from pipeloom.platform import Clock, SystemClock
from pipeloom.state import codec
from pipeloom.state.lease import Heartbeat, held, new_lock_info
from pipeloom.state.types import (
    HEARTBEAT_SECONDS,
    LEASE_SECONDS,
    LockHeld,
    LockInfo,
    RunRecord,
    StateKey,
    UnitRecord,
    Watermark,
    page_runs,
)
from pipeloom_spark import errors
from pipeloom_spark.spec import TableStateOptions
from pipeloom_spark.sqltext import literal, quote_ident

SCHEMA_VERSION: Final = 1
SCHEMA_PROPERTY: Final = "pipeloom.state.schema"
CONFLICT_RETRIES: Final = 10
"""Retries of a mutation that lost a Delta concurrent-modification race."""
JITTER: Final = (0.05, 0.5)
"""Seconds to wait before a retry, drawn uniformly."""

COLUMNS: Final = (
    ("kind", "STRING"),
    ("key", "STRING"),
    ("project", "STRING"),
    ("target", "STRING"),
    ("model", "STRING"),
    ("run_id", "STRING"),
    ("unit_id", "STRING"),
    ("scope", "STRING"),
    ("status", "STRING"),
    ("owner", "STRING"),
    ("dtype", "STRING"),
    ("attempt", "INT"),
    ("value", "STRING"),
    ("cas_key", "STRING"),
    ("started_us", "BIGINT"),
    ("ended_us", "BIGINT"),
    ("acquired_us", "BIGINT"),
    ("expires_us", "BIGINT"),
    ("updated_us", "BIGINT"),
    ("data", "STRING"),
)
_NAMES: Final = tuple(n for n, _ in COLUMNS)
_SOURCE_DDL: Final = ", ".join(f"{n} {t}" for n, t in COLUMNS) + ", expected_key STRING"
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)

T = TypeVar("T")

# -- SQL text (pure) -----------------------------------------------------------------------------


def create_sql(ident: str) -> str:
    cols = ", ".join(f"{quote_ident(n)} {t}" for n, t in COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {ident} ({cols}) USING DELTA PARTITIONED BY (kind) "
        f"TBLPROPERTIES ('{SCHEMA_PROPERTY}' = '{SCHEMA_VERSION}')"
    )


_SET: Final = ", ".join(f"t.{n} = s.{n}" for n in _NAMES)
_INSERT: Final = f"({', '.join(_NAMES)}) VALUES ({', '.join(f's.{n}' for n in _NAMES)})"
UPSERT: Final = f"WHEN MATCHED THEN UPDATE SET {_SET} WHEN NOT MATCHED THEN INSERT {_INSERT}"
CAS: Final = (
    "WHEN MATCHED AND (t.cas_key <=> s.expected_key OR (t.cas_key = s.cas_key AND "
    f"t.run_id = s.run_id)) THEN UPDATE SET {_SET} "
    f"WHEN NOT MATCHED AND s.expected_key IS NULL THEN INSERT {_INSERT}"
)
"""Swap when the stored value equals the expected one; an equal value of the same run is a
replay and succeeds too."""
TAKE_LOCK: Final = (
    f"WHEN MATCHED AND (t.expires_us <= s.acquired_us OR t.owner = s.owner) THEN UPDATE SET "
    f"{_SET} WHEN NOT MATCHED THEN INSERT {_INSERT}"
)
"""Take a free or expired lease; `acquired_us` is the taker's now. The owner is unique per
acquisition, so a retry whose first attempt committed unseen finds its own row and succeeds."""


def merge_sql(ident: str, kind: str, clauses: str) -> str:
    """The literal partition filter keeps Delta from reading, and so conflicting on, the other
    kinds' partitions."""
    return (
        f"MERGE INTO {ident} AS t USING {{src}} AS s ON t.kind = '{kind}' AND t.kind = s.kind "
        f"AND t.key = s.key {clauses}"
    )


def _where(kind: str, key: str, owner: str) -> str:
    return (
        f"kind = {literal(kind, 'string')} AND key = {literal(key, 'string')} "
        f"AND owner = {literal(owner, 'string')}"
    )


def renew_sql(ident: str, key: str, owner: str, expires_us: int, data: str) -> str:
    return (
        f"UPDATE {ident} SET expires_us = {expires_us}, updated_us = {expires_us}, "
        f"data = {literal(data, 'string')} WHERE {_where('lock', key, owner)}"
    )


def release_sql(ident: str, key: str, owner: str) -> str:
    return f"DELETE FROM {ident} WHERE {_where('lock', key, owner)}"


# -- the store -----------------------------------------------------------------------------------


def _us(value: datetime | None) -> int | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    delta = value - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds


def _key(*parts: Any) -> str:
    return codec.dumps(list(parts))


class DeltaStateStore:
    """A `StateStore` on one Delta table; `shared=True`."""

    def __init__(
        self,
        opts: TableStateOptions,
        *,
        clock: Clock | None = None,
        env: Mapping[str, str] | None = None,
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
        lease_seconds: float = LEASE_SECONDS,
    ) -> None:
        self._opts = opts
        self._clock: Clock = clock or SystemClock()
        self._env = os.environ if env is None else env
        self._heartbeat = heartbeat_seconds
        self.heartbeat = Heartbeat()
        """`heartbeat.last_error` is the last failed lease renewal, for the run to warn about."""
        self._lease = lease_seconds
        self._ident = (
            f"delta.{quote_ident(opts.path)}" if opts.path is not None else str(opts.table)
        )
        self._ready = threading.Lock()
        self._spark_obj: Any = None
        self._owned = ExitStack()

    # -- session and table ----------------------------------------------------------------------

    def _spark(self) -> Any:
        """The session, and the table created on first use (the store is built before the
        engine opens its session)."""
        with self._ready:
            if self._spark_obj is None:
                from pipeloom_spark.session import active_session, open_session

                spark = active_session()
                if spark is None:
                    spark = self._owned.enter_context(
                        open_session(self._opts.session, self._env)
                    ).spark
                self._create(spark)
                self._spark_obj = spark
            return self._spark_obj

    def _create(self, spark: Any) -> None:
        try:
            spark.sql(create_sql(self._ident)).collect()
        except Exception:
            # Another process created it first, or the create failed: the version check tells.
            if self._version(spark) is None:
                raise
        version = self._version(spark)
        if version is not None and version > SCHEMA_VERSION:
            raise RuntimeError(
                f"{self._ident} has state schema {version}; this pipeloom reads {SCHEMA_VERSION}"
            )

    def _version(self, spark: Any) -> int | None:
        try:
            rows = spark.sql(f"SHOW TBLPROPERTIES {self._ident}").collect()
        except Exception:  # noqa: BLE001 - no table yet
            return None
        props = {r[0]: r[1] for r in rows}
        return int(props[SCHEMA_PROPERTY]) if SCHEMA_PROPERTY in props else 0

    def _retry(self, fn: Callable[[], T]) -> T:
        for attempt in range(CONFLICT_RETRIES + 1):
            try:
                return fn()
            except Exception as exc:
                if attempt == CONFLICT_RETRIES or errors.is_transient(exc) is not True:
                    raise
                time.sleep(random.uniform(*JITTER))
        raise AssertionError("unreachable")

    def _merge(self, clauses: str, row: Mapping[str, Any]) -> int:
        """Run one MERGE with `row` as the source; the number of rows it changed."""
        spark = self._spark()
        values = tuple(row.get(n) for n in (*_NAMES, "expected_key"))
        text = merge_sql(self._ident, row["kind"], clauses)

        def run() -> int:
            src = spark.createDataFrame([values], _SOURCE_DDL)
            return int(spark.sql(text, src=src).first()[0])

        return self._retry(run)

    def _exec(self, text: str) -> None:
        spark = self._spark()
        self._retry(lambda: spark.sql(text).collect())

    def _get(self, kind: str, key: str) -> dict[str, Any] | None:
        rows = self._select(kind, key=key)
        return None if not rows else rows[0]

    def _select(self, kind: str, **eq: str) -> list[dict[str, Any]]:
        from pyspark.sql import functions as F

        spark = self._spark()
        if self._opts.path is not None:
            frame = spark.read.format("delta").load(self._opts.path)
        else:
            frame = spark.read.table(str(self._opts.table))
        cond = F.col("kind") == kind
        for column, value in eq.items():
            cond = cond & (F.col(column) == value)
        return [r.asDict() for r in frame.where(cond).select("data", "key").collect()]

    # -- StateStore -----------------------------------------------------------------------------

    def get_watermark(self, key: StateKey) -> Watermark | None:
        row = self._get("watermark", _key(key.project, key.target, key.model))
        return None if row is None else codec.watermark_from_json(json.loads(row["data"]))

    def cas_watermark(self, key: StateKey, expected: Watermark | None, new: Watermark) -> bool:
        row = {
            "kind": "watermark",
            "key": _key(key.project, key.target, key.model),
            "project": key.project,
            "target": key.target,
            "model": key.model,
            "run_id": new.run_id,
            "dtype": new.dtype,
            "value": codec.dumps(codec.encode_value(new.value)),
            "cas_key": codec.value_key(new),
            "updated_us": _us(new.updated_at),
            "data": codec.dumps(codec.watermark_to_json(new)),
            "expected_key": None if expected is None else codec.value_key(expected),
        }
        return self._merge(CAS, row) > 0

    def record_run(self, record: RunRecord) -> None:
        self._merge(
            UPSERT,
            {
                "kind": "run",
                "key": _key(record.run_id),
                "project": record.project,
                "target": record.target,
                "run_id": record.run_id,
                "status": record.status,
                "started_us": _us(record.started_at),
                "ended_us": _us(record.ended_at),
                "updated_us": _us(self._clock.now()),
                "data": codec.dumps(codec.run_to_json(record)),
            },
        )

    def get_run(self, run_id: str) -> RunRecord | None:
        row = self._get("run", _key(run_id))
        return None if row is None else codec.run_from_json(json.loads(row["data"]))

    def list_runs(
        self, project: str, target: str, limit: int, before: str | None = None
    ) -> tuple[RunRecord, ...]:
        rows = self._select("run", project=project, target=target)
        runs = (codec.run_from_json(json.loads(r["data"])) for r in rows)
        return page_runs(runs, project, target, limit, before)

    def record_unit(self, record: UnitRecord) -> None:
        self._merge(
            UPSERT,
            {
                "kind": "unit",
                "key": _key(record.run_id, record.unit_id, record.attempt),
                "run_id": record.run_id,
                "unit_id": record.unit_id,
                "attempt": record.attempt,
                "status": record.status,
                "started_us": _us(record.started_at),
                "ended_us": _us(record.ended_at),
                "updated_us": _us(self._clock.now()),
                "data": codec.dumps(codec.unit_to_json(record)),
            },
        )

    def list_units(self, run_id: str) -> tuple[UnitRecord, ...]:
        units = [
            codec.unit_from_json(json.loads(r["data"])) for r in self._select("unit", run_id=run_id)
        ]
        return tuple(sorted(units, key=lambda u: (u.unit_id, u.attempt)))

    def lock(self, scope: str, run_id: str) -> AbstractContextManager[LockInfo]:
        info = new_lock_info(scope, run_id, self._clock, self._lease)
        row = {
            "kind": "lock",
            "key": _key(scope),
            "scope": scope,
            "run_id": run_id,
            "owner": info.owner,
            "acquired_us": _us(info.acquired_at),
            "expires_us": _us(info.expires_at),
            "updated_us": _us(info.acquired_at),
            "data": codec.dumps(codec.lock_to_json(info)),
        }
        while self._merge(TAKE_LOCK, row) == 0:
            current = self._get("lock", _key(scope))
            if current is not None:  # else it was released in between: try again
                raise LockHeld(codec.lock_from_json(json.loads(current["data"])))
        return held(info, self._renew, self._release, self._heartbeat, self.heartbeat)

    def _renew(self, info: LockInfo) -> None:
        expires = self._clock.now() + timedelta(seconds=self._lease)
        renewed = LockInfo(info.scope, info.run_id, info.owner, info.acquired_at, expires)
        data = codec.dumps(codec.lock_to_json(renewed))
        key = _key(info.scope)
        self._exec(renew_sql(self._ident, key, info.owner, _us(expires) or 0, data))

    def _release(self, info: LockInfo) -> None:
        self._exec(release_sql(self._ident, _key(info.scope), info.owner))

    def close(self) -> None:
        """Stop the session only when this store built it."""
        self._spark_obj = None
        self._owned.close()


def path_store(location: str) -> DeltaStateStore:
    """A store on the path table at `location`; the contract's cross-process lock test uses it."""
    return DeltaStateStore(TableStateOptions(path=location))
