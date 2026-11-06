"""Retry policy: which failures retry, and when (architecture section 8)."""

from __future__ import annotations

import random
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Final, Literal

from pipeloom.engine.errors import TransientError
from pipeloom.engine.protocols import EngineSession

Category = Literal["transient", "permanent"]
Phase = Literal["read", "body", "write", "state"]

BACKOFF_CAP: Final = 300.0
"""Seconds. The cap is `max(base, BACKOFF_CAP)`, so a larger base is never cut below itself."""
_CHAIN_DEPTH: Final = 10
_NOT_TRANSIENT_OS: Final = (
    FileNotFoundError,
    PermissionError,
    IsADirectoryError,
    NotADirectoryError,
    FileExistsError,
)
PERMANENT_CODES: Final = frozenset(
    {"PLQ301", "PLQ302", "PLM202", "PLM203", "PLM205", "PLM206", "PLM207", "PLM208"}
)
"""Failures that repeat on the same data; only `retry_on: any` retries them."""


@dataclass(frozen=True)
class UnitPolicy:
    retries: int
    retry_delay: float
    retry_on: Literal["transient", "any"]
    timeout: float | None


NO_POLICY: Final = UnitPolicy(0, 0.0, "transient", None)


def _chain(exc: BaseException) -> Iterator[BaseException]:
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and len(seen) < _CHAIN_DEPTH and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        cur = cur.__cause__ or (None if cur.__suppress_context__ else cur.__context__)


def _transient_os(exc: OSError) -> bool:
    if isinstance(exc, PermissionError):  # a Windows sharing violation clears by itself
        return sys.platform == "win32" and getattr(exc, "winerror", None) in (32, 33)
    return not isinstance(exc, _NOT_TRANSIENT_OS)


def classify(exc: BaseException, *, phase: Phase, session: EngineSession | None) -> Category:
    """Order: `TransientError` in the cause chain, the engine's opinion, connection and timeout
    errors, then `OSError` while reading, writing or touching state; anything else is permanent."""
    chain = list(_chain(exc))
    if any(isinstance(e, TransientError) for e in chain):
        return "transient"
    if session is not None:
        opinion = session.is_transient(exc)
        if opinion is not None:
            return "transient" if opinion else "permanent"
    if any(isinstance(e, ConnectionError | TimeoutError) for e in chain):
        return "transient"
    if phase != "body":
        for e in chain:
            if isinstance(e, OSError):
                return "transient" if _transient_os(e) else "permanent"
    return "permanent"


def backoff_delay(base: float, retry_number: int, *, seed: str) -> float:
    """Exponential backoff with a cap and jitter in [d/2, d]; `seed` makes it reproducible."""
    d = min(max(base, BACKOFF_CAP), base * float(2 ** (retry_number - 1)))
    jitter: float = random.Random(seed).random()
    return d * (0.5 + 0.5 * jitter)


def should_retry(policy: UnitPolicy, used: int, category: Category | None) -> bool:
    """`used` retries were taken already. Timed-out and cancelled attempts never get here."""
    if used >= policy.retries:
        return False
    return policy.retry_on == "any" or category == "transient"
