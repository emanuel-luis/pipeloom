"""Compute an ephemeral model once for every unit that inlines it (thread executor only)."""

from __future__ import annotations

import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from pipeloom.engine.types import Relation


@dataclass
class _Entry:
    lock: threading.Lock = field(default_factory=threading.Lock)
    done: bool = False
    value: Relation | None = None
    error: BaseException | None = None


class EphemeralMemo:
    """`consumers` maps an ephemeral's unique id to the selected units that inline it.

    The first unit to ask computes it, the others wait for that result. A failure is cached too,
    so every consumer fails the same way. With more than one consumer the result goes through
    `persist`, so a lazy relation is not recomputed by each. An entry is dropped when the last
    consuming unit calls `release`.
    """

    def __init__(
        self,
        consumers: Mapping[str, int],
        persist: Callable[[Relation], Relation] | None = None,
    ) -> None:
        self._left = dict(consumers)
        self._persist = persist
        self._entries: dict[str, _Entry] = {}
        self._used: dict[str, set[str]] = {}
        self._lock = threading.Lock()

    def get(self, uid: str, compute: Callable[[], Relation], *, unit_id: str) -> Relation:
        with self._lock:
            entry = self._entries.setdefault(uid, _Entry())
            self._used.setdefault(unit_id, set()).add(uid)
        with entry.lock:
            if not entry.done:
                try:
                    rel = compute()
                    if self._persist is not None and self._left.get(uid, 1) > 1:
                        rel = self._persist(rel)
                    entry.value = rel
                except BaseException as exc:  # noqa: BLE001 - cached and re-raised to every consumer
                    entry.error = exc
                entry.done = True
        if entry.error is not None:
            raise entry.error
        assert entry.value is not None
        return entry.value

    def release(self, unit_id: str) -> None:
        """The unit is done: drop each entry it used whose consumers have all finished."""
        with self._lock:
            for uid in self._used.pop(unit_id, ()):
                self._left[uid] = self._left.get(uid, 1) - 1
                if self._left[uid] <= 0:
                    self._entries.pop(uid, None)

    def __len__(self) -> int:
        return len(self._entries)
