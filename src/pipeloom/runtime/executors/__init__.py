"""The executor protocol: how a scheduled unit is actually run."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol

from pipeloom.runtime.control import Reason
from pipeloom.runtime.retry import Category

if TYPE_CHECKING:
    from pipeloom.runtime.runner import ModelResult


@dataclass(frozen=True)
class UnitHandle:
    unit_id: str
    seq: int
    """Increases with every submit of an executor, so a retry has a new handle."""
    attempt: int = 1
    """The attempt number this submit will write."""


@dataclass(frozen=True)
class UnitCompletion:
    handle: UnitHandle
    status: Literal["succeeded", "failed", "timed_out", "cancelled"]
    attempt: int
    category: Category | None = None
    result: ModelResult | None = None
    """What the runner reported; None when the completion carries an internal error."""
    internal_error: BaseException | None = None
    """A bug in the runner or executor, not a failed unit. The scheduler stops and re-raises."""

    @property
    def unit_id(self) -> str:
        return self.handle.unit_id


class Executor(Protocol):
    name: str
    boundary: Literal["process", "task"]

    def submit(self, unit_id: str, *, queued_at: float) -> UnitHandle:
        """Start a unit. `queued_at` is the scheduler clock's monotonic time it became ready."""
        ...

    def wait_any(self, timeout: float | None) -> UnitCompletion | None:
        """Block until a unit completes; None when `timeout` seconds pass or `wake` is called."""
        ...

    def cancel(self, handle: UnitHandle, reason: Reason, *, final: bool = False) -> bool:
        """Ask a running unit to stop. A `timeout`, or a `final` cancel (the end of the grace),
        also abandons it: its completion is dropped.

        False when a final verdict came too late: the unit already has its result and its completion
        is still coming, so the scheduler must wait for it."""
        ...

    def has_terminal_event(self, handle: UnitHandle) -> bool:
        """True when the unit already emitted its own terminal event (a child process did)."""
        ...

    def wake(self) -> None:
        """Make a blocked `wait_any` return. Reentrant: safe from a signal handler."""
        ...

    def shutdown(self, *, wait: bool) -> None: ...
