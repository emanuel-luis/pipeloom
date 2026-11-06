"""Records and the `StateStore` protocol."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
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
        """Store `new` when the stored watermark equals `expected` on (value, dtype).

        Also true, without a write, when the stored watermark already equals `new` and was
        written by the same run (a replay). Another run's equal value is a lost race."""
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

    def list_runs(
        self, project: str, target: str, limit: int, before: str | None = None
    ) -> tuple[RunRecord, ...]:
        """The runs of `project` and `target`, newest first by (started_at, run_id), at most
        `limit`. With `before`, the runs that come after that run id; `UnknownRun` when it names
        no run of this project and target. Plugin API (0, 5); `page_runs` does the ordering."""
        ...

    def lock(self, scope: str, run_id: str) -> AbstractContextManager[LockInfo]:
        """Take the lock without waiting; raise `LockHeld` when it is held. Renewed while held."""
        ...

    def close(self) -> None: ...


class UnknownRun(LookupError):
    """A run id names no run of the project and target."""

    def __init__(self, run_id: str) -> None:
        super().__init__(f"run {run_id} not found")
        self.run_id = run_id


class ListRunsUnsupported(Exception):
    """The state store has no `list_runs` (it was written before plugin API (0, 5))."""


def page_runs(
    runs: Iterable[RunRecord], project: str, target: str, limit: int, before: str | None = None
) -> tuple[RunRecord, ...]:
    """What `StateStore.list_runs` returns, from every run record a store holds."""
    if limit < 1:
        raise ValueError("limit must be at least 1")
    mine = sorted(
        (r for r in runs if r.project == project and r.target == target),
        key=lambda r: (r.started_at, r.run_id),
        reverse=True,
    )
    if before is not None:
        ids = [r.run_id for r in mine]
        if before not in ids:
            raise UnknownRun(before)
        mine = mine[ids.index(before) + 1 :]
    return tuple(mine[:limit])


def list_runs(
    store: object, project: str, target: str, limit: int, before: str | None = None
) -> tuple[RunRecord, ...]:
    """Call `store.list_runs`; `ListRunsUnsupported` when the store lacks it. Plugins implement
    the protocol structurally, so this is where the default lives."""
    method = getattr(store, "list_runs", None)
    if method is None:
        raise ListRunsUnsupported(f"state store {type(store).__name__} cannot list runs")
    runs: tuple[RunRecord, ...] = method(project, target, limit, before)
    return runs
