"""The executor protocol: how a scheduled unit is actually run."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass(frozen=True)
class UnitHandle:
    unit_id: str


@dataclass(frozen=True)
class UnitCompletion:
    unit_id: str
    status: Literal["succeeded", "failed"]
    result: object | None = None
    """The runner's `ModelResult`; typed loosely until the runner lands."""
    internal_error: BaseException | None = None
    """A bug in the runner or executor, not a failed unit. The scheduler stops and re-raises."""


class Executor(Protocol):
    name: str
    boundary: Literal["process", "task"]

    def submit(self, unit_id: str, *, queued_at: float) -> UnitHandle:
        """Start a unit. `queued_at` is the scheduler clock's monotonic time it became ready."""
        ...

    def wait_any(self) -> UnitCompletion:
        """Block until one submitted unit completes. Only called while a unit is running."""
        ...

    def cancel(self, handle: UnitHandle) -> bool:
        """Ask a running unit to stop; False when it cannot. Not called before 0.3."""
        ...

    def shutdown(self, *, wait: bool) -> None: ...
