"""Records and the `StateStore` protocol."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, Literal, Protocol, runtime_checkable

from pipeloom.diagnostics import JsonValue

LEASE_SECONDS: Final = 60
"""A lock is valid this long after it was taken or last renewed."""
HEARTBEAT_SECONDS: Final = 20
"""A held lock is renewed this often by a daemon thread."""

RunStatus = Literal["created", "compiled", "running", "succeeded", "failed", "cancelled"]


@dataclass(frozen=True)
class StateKey:
    project: str
    target: str
    model: str


@dataclass(frozen=True)
class Watermark:
    value: Any
    dtype: str
    """Logical type, for example int64, string, date, timestamp[UTC] or decimal(18,2)."""
    run_id: str
    updated_at: datetime


@dataclass(frozen=True)
class RunRecord:
    run_id: str
    project: str
    target: str
    plan_hash: str
    status: RunStatus
    started_at: datetime
    ended_at: datetime | None = None
    parent_run_id: str | None = None
    invocation_ids: tuple[str, ...] = ()
    counts: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class UnitRecord:
    run_id: str
    unit_id: str
    attempt: int
    status: str
    invocation_id: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    data: Mapping[str, JsonValue] = field(default_factory=dict)


@dataclass(frozen=True)
class LockInfo:
    scope: str
    run_id: str
    owner: str
    """`<host>:<pid>:<uuid8>`."""
    acquired_at: datetime
    expires_at: datetime


class LockHeld(Exception):
    """The lock is held by someone else and its lease has not expired."""

    def __init__(self, info: LockInfo) -> None:
        super().__init__(
            f"lock '{info.scope}' is held by {info.owner} (run {info.run_id}) "
            f"until {info.expires_at.isoformat()}"
        )
        self.info = info


@runtime_checkable
class StateStore(Protocol):
    def get_watermark(self, key: StateKey) -> Watermark | None: ...

    def cas_watermark(self, key: StateKey, expected: Watermark | None, new: Watermark) -> bool:
        """Store `new` when the stored watermark equals `expected` on (value, dtype)."""
        ...

    def record_run(self, record: RunRecord) -> None:
        """Insert or replace the record of `record.run_id`."""
        ...

    def get_run(self, run_id: str) -> RunRecord | None: ...

    def record_unit(self, record: UnitRecord) -> None:
        """Insert or replace the record of (run_id, unit_id, attempt)."""
        ...

    def list_units(self, run_id: str) -> tuple[UnitRecord, ...]:
        """Sorted by (unit_id, attempt)."""
        ...

    def lock(self, scope: str, run_id: str) -> AbstractContextManager[LockInfo]:
        """Take the lock without waiting; raise `LockHeld` when it is held. Renewed while held."""
        ...

    def close(self) -> None: ...
