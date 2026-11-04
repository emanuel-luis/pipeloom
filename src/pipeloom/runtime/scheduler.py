"""The ready-queue scheduler (ADR-0006, ADR-0020, ADR-0029)."""

from __future__ import annotations

import heapq
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal, Self

from pipeloom.events.bus import EventBus
from pipeloom.events.model import (
    EventPayload,
    RunCancelRequested,
    UnitCancelled,
    UnitQueued,
    UnitRetrying,
    UnitSkipped,
    UnitTimedOut,
)
from pipeloom.plan.model import Plan
from pipeloom.platform import Clock
from pipeloom.runtime.control import Reason, RunControl, interrupt_error
from pipeloom.runtime.executors import Executor, UnitCompletion, UnitHandle
from pipeloom.runtime.retry import NO_POLICY, UnitPolicy, backoff_delay, should_retry
from pipeloom.runtime.runner import ModelResult
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
    """The last completion of every unit that ran; a timed-out or cancelled unit gets a verdict."""
    trace: tuple[tuple[float, str, UnitState], ...]
    """Every state change after the initial PENDING as (clock.monotonic(), unit, new state)."""


_SLACK = 1e-6
"""Seconds. Clocks tick in microseconds, so a deadline within a tick counts as reached."""


@dataclass
class _Run:
    handle: UnitHandle
    deadline: float | None
    finished: bool = False
    """The runner has its result: a verdict came too late and the completion is on its way."""


class Scheduler:
    """Starts a unit as soon as its selected parents succeeded, highest downstream count first.

    Retries, timeouts and stop requests are state driven by `clock.monotonic()` and
    `Executor.wait_any(timeout)`; the scheduler never sleeps. A failed attempt that may retry
    moves RUNNING -> RETRYING and holds no worker slot while it waits. A deadline passed moves a
    unit to TIMED_OUT (never retried). Under `boundary: task` the child enforces the timeout, so
    the scheduler's own deadline is `timeout + cancel_grace`.

    Events `unit.queued`, `unit.skipped` and `unit.cancelled` depend only on the order in which
    completions arrive. `unit.started` and the terminal events of finished attempts belong to the
    runner. `run` raises the first internal error after every running unit finished; the full
    result is then in `self.result`. The caller owns the executor and shuts it down.
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
        policies: Mapping[str, UnitPolicy] | None = None,
        control: RunControl | None = None,
        cancel_grace: float = 30.0,
        run_id: str = "",
        poll_interval: float = 0.25,
        announce: bool = True,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self._graph = graph
        self._executor = executor
        self._workers = workers
        self._fail_fast = fail_fast
        self._bus = bus
        self._clock = clock
        self._policies = policies or {}
        self._control = control or RunControl()
        self._grace = cancel_grace
        self._run_id = run_id
        self._poll = poll_interval
        self._announce = announce
        """False in unit mode: the invocation that owns the run emits the scheduler events."""
        self.result: ScheduleResult | None = None
        self.interrupted = False
        """True once a stop was requested or a KeyboardInterrupt stopped the run."""

    def request_stop(self, *, hard: bool = False) -> None:
        """Stop dequeuing; pending and queued units become cancelled. Safe from any thread."""
        self._control.request_stop(hard=hard)
        self._executor.wake()

    def _policy(self, unit_id: str) -> UnitPolicy:
        return self._policies.get(unit_id, NO_POLICY)

    def run(self) -> ScheduleResult:
        g = self._graph
        self._downstream = g.downstream_counts()
        self._index = {u: i for i, u in enumerate(g.units)}
        self._waiting = {u: len(g.parents[u]) for u in g.units}
        self._states = dict.fromkeys(g.units, UnitState.PENDING)
        self._ready: list[tuple[int, str]] = []
        self._retry: list[tuple[float, str]] = []
        self._retrying = 0
        self._retries_used: dict[str, int] = {}
        self._queued_at: dict[str, float] = {}
        self._run_of: dict[str, _Run] = {}
        self._skipped: dict[str, tuple[SkipReason, str | None]] = {}
        self._completions: dict[str, UnitCompletion] = {}
        self._trace: list[tuple[float, str, UnitState]] = []
        self._halted = False
        self._stopping = False
        self._stop_by: float | None = None
        error: BaseException | None = None
        for u in g.units:
            if self._waiting[u] == 0:
                self._queue(u)
        keyboard = False
        self._control.add_waker(self._executor.wake)
        try:
            while True:
                now = self._clock.monotonic()
                self._check_stop(now)
                if not self._halted:
                    self._promote(now)
                    self._fill(now)
                if not self._run_of and not self._retrying:
                    break
                try:
                    comp = self._executor.wait_any(self._wait_for(now))
                except KeyboardInterrupt:
                    # The first Ctrl+C stops dequeuing and lets running units finish; a second
                    # one propagates.
                    if keyboard:
                        raise
                    keyboard = True
                    self.interrupted = True
                    self._halt()
                    continue
                if comp is not None:
                    error = error or self._take(comp)
                    # A completion due at the deadline wins over the timeout.
                    while (more := self._executor.wait_any(0)) is not None:
                        error = error or self._take(more)
                self._deadlines(self._clock.monotonic())
        finally:
            self._control.remove_waker(self._executor.wake)
            # Also set when an exception escapes, so the caller can finalize what happened.
            self.result = ScheduleResult(
                states=dict(self._states),
                skipped=dict(self._skipped),
                completions=dict(self._completions),
                trace=tuple(self._trace),
            )
        if error is not None:
            raise error
        return self.result

    def _take(self, comp: UnitCompletion) -> BaseException | None:
        failure = self._complete(comp)
        if failure is not None:
            self._halt()
        return failure

    def _emit(self, unit_id: str, payload: EventPayload) -> None:
        if self._announce:
            self._bus.emit(payload, unit_id=unit_id, model=unit_id)

    def _emit_attempt(self, unit_id: str, payload: EventPayload, attempt: int) -> None:
        """Attempt events (retrying, timed out) are emitted in every mode."""
        self._bus.emit(payload, unit_id=unit_id, model=unit_id, attempt=attempt)

    def _set(self, unit_id: str, new: UnitState) -> None:
        old = self._states[unit_id]
        check_transition(old, new)
        self._retrying += (new is UnitState.RETRYING) - (old is UnitState.RETRYING)
        self._states[unit_id] = new
        self._trace.append((self._clock.monotonic(), unit_id, new))

    def _queue(self, unit_id: str) -> None:
        self._set(unit_id, UnitState.QUEUED)
        self._queued_at[unit_id] = self._clock.monotonic()
        heapq.heappush(self._ready, (-self._downstream[unit_id], unit_id))
        self._emit(unit_id, UnitQueued(downstream=self._downstream[unit_id]))

    def _promote(self, now: float) -> None:
        self._trim_retry()
        while self._retry and self._retry[0][0] <= now + _SLACK:
            _, u = heapq.heappop(self._retry)
            if self._states[u] is UnitState.RETRYING:
                self._queued_at[u] = now
                heapq.heappush(self._ready, (-self._downstream[u], u))

    def _trim_retry(self) -> None:
        """Drop heap heads of units that were cancelled while they waited."""
        while self._retry and self._states[self._retry[0][1]] is not UnitState.RETRYING:
            heapq.heappop(self._retry)

    def _fill(self, now: float) -> None:
        while len(self._run_of) < self._workers and self._ready:
            _, u = heapq.heappop(self._ready)
            if self._states[u] not in (UnitState.QUEUED, UnitState.RETRYING):
                continue  # skipped or cancelled while it waited
            self._set(u, UnitState.RUNNING)
            handle = self._executor.submit(u, queued_at=self._queued_at[u])
            limit = self._policy(u).timeout
            if limit is not None and self._executor.boundary == "task":
                limit += self._grace  # the child enforces the limit itself
            self._run_of[u] = _Run(handle, None if limit is None else now + limit)

    def _wait_for(self, now: float) -> float:
        due = [now + self._poll]
        due += [r.deadline for r in self._run_of.values() if r.deadline is not None]
        self._trim_retry()
        if self._retry:
            due.append(self._retry[0][0])
        if self._stop_by is not None and self._stop_by > now:
            due.append(self._stop_by)
        return max(0.0, min(due) - now)

    # -- completions ------------------------------------------------------------------------

    def _complete(self, comp: UnitCompletion) -> BaseException | None:
        u = comp.handle.unit_id
        run = self._run_of.get(u)
        if run is None or run.handle != comp.handle:
            return None  # a late result of an attempt that was timed out or cancelled
        del self._run_of[u]
        self._completions[u] = comp
        if comp.internal_error is not None:
            self._set(u, UnitState.FAILED)
            self._skip_descendants(u)
            return comp.internal_error
        if comp.status == "succeeded":
            self._set(u, UnitState.SUCCEEDED)
            for child in self._graph.children[u]:
                if self._states[child] is UnitState.PENDING:
                    self._waiting[child] -= 1
                    if self._waiting[child] == 0:
                        self._queue(child)
            return None
        if comp.status == "cancelled":
            self._set(u, UnitState.CANCELLED)
            if not self._executor.has_terminal_event(comp.handle):
                self._emit(u, UnitCancelled(reason="cancel"))
            return None
        if comp.status == "timed_out":
            self._set(u, UnitState.TIMED_OUT)
            if not self._executor.has_terminal_event(comp.handle):
                self._announce_timeout(u, comp.attempt)
            self._after_failure(u)
            return None
        used = self._retries_used.get(u, 0)
        policy = self._policy(u)
        if not self._halted and should_retry(policy, used, comp.category):
            n = used + 1
            self._retries_used[u] = n
            delay = backoff_delay(policy.retry_delay, n, seed=f"{self._run_id}:{u}:{n}")
            self._set(u, UnitState.RETRYING)
            heapq.heappush(self._retry, (self._clock.monotonic() + delay, u))
            self._emit_attempt(
                u,
                UnitRetrying(
                    attempt=comp.attempt,
                    next_attempt=comp.attempt + 1,
                    delay_ms=int(delay * 1000),
                    error=None if comp.result is None else comp.result.error,
                ),
                comp.attempt,
            )
            return None
        self._set(u, UnitState.FAILED)
        self._after_failure(u)
        return None

    def _after_failure(self, unit_id: str) -> None:
        self._skip_descendants(unit_id)
        if self._fail_fast:
            self._skip_rest()

    def _announce_timeout(self, unit_id: str, attempt: int) -> None:
        limit = self._policy(unit_id).timeout
        self._emit_attempt(unit_id, UnitTimedOut(timeout_s=limit or 0.0, attempt=attempt), attempt)

    def _verdict(self, unit_id: str, run: _Run, reason: Reason) -> None:
        """The scheduler's own verdict for an attempt it gave up on."""
        h = run.handle
        err = interrupt_error(reason, unit_id, h.attempt, self._policy(unit_id).timeout)
        status: Literal["timed_out", "cancelled"] = (
            "timed_out" if reason == "timeout" else "cancelled"
        )
        self._completions[unit_id] = UnitCompletion(
            h, status, h.attempt, "permanent", ModelResult(unit_id, status, h.attempt, {}, err)
        )

    def _deadlines(self, now: float) -> None:
        for u, run in list(self._run_of.items()):
            if run.deadline is None or now + _SLACK < run.deadline:
                continue
            if run.finished:
                continue
            if not self._executor.cancel(run.handle, "timeout"):
                run.finished = True  # finished just in time: its completion decides
                run.deadline = None
                continue
            del self._run_of[u]
            self._verdict(u, run, "timeout")
            self._set(u, UnitState.TIMED_OUT)
            if not self._executor.has_terminal_event(run.handle):
                self._announce_timeout(u, run.handle.attempt)
            self._after_failure(u)

    # -- stopping ---------------------------------------------------------------------------

    def _halt(self) -> None:
        if not self._halted:
            self._halted = True
            self._cancel_rest()

    def _check_stop(self, now: float) -> None:
        control = self._control
        if not control.stop_requested:
            return
        if not self._stopping:
            self._stopping = True
            self.interrupted = True
            self._halt()
            self._stop_by = now + self._grace
            if self._announce:
                self._bus.emit(RunCancelRequested(signal=control.signal_name, grace_s=self._grace))
            for run in self._run_of.values():
                self._executor.cancel(run.handle, "cancel")
        if self._run_of and (
            control.hard or (self._stop_by is not None and now + _SLACK >= self._stop_by)
        ):
            gone = False
            for u, run in list(self._run_of.items()):
                if run.finished:
                    continue
                if not self._executor.cancel(run.handle, "cancel", final=True):
                    run.finished = True  # it has its result: keep waiting for it
                    run.deadline = None
                    continue
                del self._run_of[u]
                gone = True
                self._verdict(u, run, "cancel")
                self._set(u, UnitState.CANCELLED)
                self._emit_attempt(u, UnitCancelled(reason="cancel"), run.handle.attempt)
            if gone and not self._run_of:
                self._executor.shutdown(wait=False)

    def _open(self) -> list[str]:
        return [
            u
            for u in self._graph.units
            if self._states[u] in (UnitState.PENDING, UnitState.QUEUED, UnitState.RETRYING)
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
            if self._states[u] is UnitState.RETRYING:  # it already ran: stopping it is a cancel
                self._set(u, UnitState.CANCELLED)
                self._emit(u, UnitCancelled())
            else:
                self._skip(u, "fail_fast", None)

    def _cancel_rest(self) -> None:
        for u in self._open():
            self._set(u, UnitState.CANCELLED)
            self._emit(u, UnitCancelled())


__all__ = ["TERMINAL", "ScheduleResult", "Scheduler", "UnitGraph"]
