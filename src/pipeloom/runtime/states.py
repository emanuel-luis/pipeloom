"""The unit state machine (architecture section 8)."""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Final, Literal


class UnitState(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


SkipReason = Literal["upstream_failed", "fail_fast"]

_S = UnitState

# RETRYING and TIMED_OUT, and RUNNING -> CANCELLED, are reachable only from 0.3.
TRANSITIONS: Final[Mapping[UnitState, frozenset[UnitState]]] = {
    _S.PENDING: frozenset({_S.QUEUED, _S.SKIPPED, _S.CANCELLED}),
    _S.QUEUED: frozenset({_S.RUNNING, _S.SKIPPED, _S.CANCELLED}),
    _S.RUNNING: frozenset({_S.SUCCEEDED, _S.FAILED, _S.TIMED_OUT, _S.CANCELLED, _S.RETRYING}),
    _S.RETRYING: frozenset({_S.RUNNING, _S.CANCELLED}),
    _S.SUCCEEDED: frozenset(),
    _S.FAILED: frozenset(),
    _S.TIMED_OUT: frozenset(),
    _S.CANCELLED: frozenset(),
    _S.SKIPPED: frozenset(),
}

TERMINAL: Final = frozenset(s for s, nxt in TRANSITIONS.items() if not nxt)


class IllegalTransition(Exception):
    def __init__(self, old: UnitState, new: UnitState) -> None:
        super().__init__(f"illegal unit transition {old.value} -> {new.value}")
        self.old = old
        self.new = new


def check_transition(old: UnitState, new: UnitState) -> None:
    if new not in TRANSITIONS[old]:
        raise IllegalTransition(old, new)
