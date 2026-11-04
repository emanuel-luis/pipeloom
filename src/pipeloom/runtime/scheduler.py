"""The ready-queue scheduler (ADR-0006, ADR-0020)."""

from __future__ import annotations

import heapq
import threading
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Self

from pipeloom.events.bus import EventBus
from pipeloom.events.model import EventPayload, UnitCancelled, UnitQueued, UnitSkipped
from pipeloom.plan.model import Plan
from pipeloom.platform import Clock
from pipeloom.runtime.executors import Executor, UnitCompletion
from pipeloom.runtime.states import TERMINAL, SkipReason, UnitState, check_transition


@dataclass(frozen=True)
class UnitGraph:
    """The selected units and the edges between them.

    `units` is in topological order, ties broken by name. A parent outside the selection
    counts as satisfied, so it is not an edge here.
    """

    units: tuple[str, ...]
    parents: Mapping[str, tuple[str, ...]]
    children: Mapping[str, tuple[str, ...]]

    @classmethod
    def from_plan(cls, plan: Plan, selected: Iterable[str]) -> Self:
        return cls.from_parents({u: unit.parents for u, unit in plan.units.items()}, selected)

    @classmethod
    def from_parents(
        cls, all_parents: Mapping[str, Iterable[str]], selected: Iterable[str]
    ) -> Self:
        chosen = set(selected)
        unknown = chosen - all_parents.keys()
        if unknown:
            raise ValueError(f"unknown units: {sorted(unknown)}")
        parents = {u: tuple(sorted(p for p in all_parents[u] if p in chosen)) for u in chosen}
        kids: dict[str, list[str]] = {u: [] for u in chosen}
        for u, ps in parents.items():
            for p in ps:
                kids[p].append(u)
        waiting = {u: len(ps) for u, ps in parents.items()}
        heap = [u for u, n in waiting.items() if n == 0]
        heapq.heapify(heap)
        order: list[str] = []
        while heap:
            u = heapq.heappop(heap)
            order.append(u)
            for c in sorted(kids[u]):
                waiting[c] -= 1
                if waiting[c] == 0:
                    heapq.heappush(heap, c)
        if len(order) != len(chosen):
            raise ValueError("unit graph has a cycle")
        return cls(
            units=tuple(order),
            parents={u: parents[u] for u in order},
            children={u: tuple(sorted(kids[u])) for u in order},
        )

    def downstream_counts(self) -> Mapping[str, int]:
        """Transitive descendants per unit, from int bitsets in reverse topological order."""
        index = {u: i for i, u in enumerate(self.units)}
        mask: dict[str, int] = {}
        for u in reversed(self.units):
            m = 0
            for c in self.children[u]:
                m |= (1 << index[c]) | mask[c]
            mask[u] = m
        return {u: mask[u].bit_count() for u in self.units}


@dataclass(frozen=True)
class ScheduleResult:
    states: Mapping[str, UnitState]
    skipped: Mapping[str, tuple[SkipReason, str | None]]
    """Skipped unit to (reason, failed upstream or None)."""
    completions: Mapping[str, UnitCompletion]
    trace: tuple[tuple[float, str, UnitState], ...]
    """Every state change after the initial PENDING as (clock.monotonic(), unit, new state)."""


class Scheduler:
    """Starts a unit as soon as its selected parents succeeded, highest downstream count first.

    Events (`unit.queued`, `unit.skipped`, `unit.cancelled`) depend only on the order in which
    completions arrive. `unit.started` and the terminal unit events belong to the runner.
    `run` raises the first internal error after every running unit finished; the full result is
    then in `self.result`. The caller owns the executor and shuts it down.
    """

    def __init__(
        self,
        graph: UnitGraph,
        executor: Executor,
        *,
        workers: int,
        fail_fast: bool,
        bus: EventBus,
        clock: Clock,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self._graph = graph
        self._executor = executor
        self._workers = workers
        self._fail_fast = fail_fast
        self._bus = bus
        self._clock = clock
        self._stop = threading.Event()
        self.result: ScheduleResult | None = None

    def request_stop(self) -> None:
        """Stop dequeuing; pending and queued units become cancelled. Safe from any thread."""
        self._stop.set()

    def run(self) -> ScheduleResult:
        g = self._graph
        self._downstream = g.downstream_counts()
        self._index = {u: i for i, u in enumerate(g.units)}
        self._waiting = {u: len(g.parents[u]) for u in g.units}
        self._states = dict.fromkeys(g.units, UnitState.PENDING)
        self._heap: list[tuple[int, str]] = []
        self._queued_at: dict[str, float] = {}
        self._running: set[str] = set()
        self._skipped: dict[str, tuple[SkipReason, str | None]] = {}
        self._completions: dict[str, UnitCompletion] = {}
        self._trace: list[tuple[float, str, UnitState]] = []
        error: BaseException | None = None
        halted = False
        for u in g.units:
            if self._waiting[u] == 0:
                self._queue(u)
        while True:
            if not halted and self._stop.is_set():
                halted = True
                self._cancel_rest()
            if not halted:
                self._fill()
            if not self._running:
                break
            comp = self._executor.wait_any()
            failure = self._complete(comp)
            if failure is not None:
                error = error or failure
                halted = True
                self._cancel_rest()
        self.result = ScheduleResult(
            states=dict(self._states),
            skipped=dict(self._skipped),
            completions=dict(self._completions),
            trace=tuple(self._trace),
        )
        if error is not None:
            raise error
        return self.result

    def _emit(self, unit_id: str, payload: EventPayload) -> None:
        self._bus.emit(payload, unit_id=unit_id, model=unit_id)

    def _set(self, unit_id: str, new: UnitState) -> None:
        check_transition(self._states[unit_id], new)
        self._states[unit_id] = new
        self._trace.append((self._clock.monotonic(), unit_id, new))

    def _queue(self, unit_id: str) -> None:
        self._set(unit_id, UnitState.QUEUED)
        self._queued_at[unit_id] = self._clock.monotonic()
        heapq.heappush(self._heap, (-self._downstream[unit_id], unit_id))
        self._emit(unit_id, UnitQueued(downstream=self._downstream[unit_id]))

    def _fill(self) -> None:
        while len(self._running) < self._workers and self._heap:
            _, unit_id = heapq.heappop(self._heap)
            if self._states[unit_id] is not UnitState.QUEUED:
                continue  # skipped while it waited
            self._set(unit_id, UnitState.RUNNING)
            self._running.add(unit_id)
            self._executor.submit(unit_id, queued_at=self._queued_at[unit_id])

    def _complete(self, comp: UnitCompletion) -> BaseException | None:
        u = comp.unit_id
        if u not in self._running:
            raise RuntimeError(f"executor completed {u!r}, which is not running")
        self._running.discard(u)
        self._completions[u] = comp
        if comp.status == "succeeded" and comp.internal_error is None:
            self._set(u, UnitState.SUCCEEDED)
            for child in self._graph.children[u]:
                if self._states[child] is UnitState.PENDING:
                    self._waiting[child] -= 1
                    if self._waiting[child] == 0:
                        self._queue(child)
            return None
        self._set(u, UnitState.FAILED)
        self._skip_descendants(u)
        if self._fail_fast and comp.internal_error is None:
            self._skip_rest()
        return comp.internal_error

    def _open(self) -> list[str]:
        return [
            u for u in self._graph.units if self._states[u] in (UnitState.PENDING, UnitState.QUEUED)
        ]

    def _skip(self, unit_id: str, reason: SkipReason, upstream: str | None) -> None:
        self._set(unit_id, UnitState.SKIPPED)
        self._skipped[unit_id] = (reason, upstream)
        self._emit(unit_id, UnitSkipped(reason=reason, upstream=upstream))

    def _skip_descendants(self, failed: str) -> None:
        seen: set[str] = set()
        todo = [failed]
        while todo:
            for c in self._graph.children[todo.pop()]:
                if c not in seen:
                    seen.add(c)
                    todo.append(c)
        for u in sorted(seen, key=self._index.__getitem__):
            if self._states[u] in (UnitState.PENDING, UnitState.QUEUED):
                self._skip(u, "upstream_failed", failed)

    def _skip_rest(self) -> None:
        for u in self._open():
            self._skip(u, "fail_fast", None)

    def _cancel_rest(self) -> None:
        for u in self._open():
            self._set(u, UnitState.CANCELLED)
            self._emit(u, UnitCancelled())


__all__ = ["TERMINAL", "ScheduleResult", "Scheduler", "UnitGraph"]
