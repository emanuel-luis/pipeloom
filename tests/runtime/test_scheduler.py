from __future__ import annotations

import itertools
from collections.abc import Mapping
from datetime import timedelta

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pipeloom.events.bus import EventBus
from pipeloom.plan.model import ExecutionUnit, Plan
from pipeloom.runtime.control import CancelToken
from pipeloom.runtime.executors import UnitCompletion, UnitHandle
from pipeloom.runtime.executors.thread import ThreadExecutor
from pipeloom.runtime.scheduler import Scheduler, ScheduleResult, UnitGraph
from pipeloom.runtime.states import (
    TERMINAL,
    TRANSITIONS,
    IllegalTransition,
    UnitState,
    check_transition,
)
from pipeloom.testing import FakeClock, RecordingSink
from pipeloom.testing.sim import SimExecutor, script_of

S = UnitState


def simulate(
    edges: Mapping[str, tuple[str, ...]],
    durations: Mapping[str, int],
    failures: frozenset[str] = frozenset(),
    *,
    workers: int = 1,
    fail_fast: bool = False,
    selected: tuple[str, ...] | None = None,
) -> tuple[ScheduleResult, RecordingSink]:
    clock = FakeClock(step=timedelta(0))
    sink = RecordingSink()
    bus = EventBus([(sink, 5)], clock=clock, invocation_id="inv")
    graph = UnitGraph.from_parents(edges, edges if selected is None else selected)
    sched = Scheduler(
        graph,
        SimExecutor(script_of(durations, failures), clock),
        workers=workers,
        fail_fast=fail_fast,
        bus=bus,
        clock=clock,
    )
    return sched.run(), sink


def stream(sink: RecordingSink) -> list[tuple[int, str, str | None, dict[str, object]]]:
    return [(e.seq, e.type, e.unit_id, dict(e.data)) for e in sink.events]


def starts(result: ScheduleResult) -> list[tuple[float, str]]:
    return [(t, u) for t, u, s in result.trace if s is S.RUNNING]


# --- explicit examples -------------------------------------------------------------------


def test_no_level_barrier() -> None:
    edges = {"a": (), "b": ("a",), "c": ()}
    result, _ = simulate(edges, {"a": 1, "b": 1, "c": 10}, workers=2)
    assert (1.0, "b") in starts(result)
    assert all(s is S.SUCCEEDED for s in result.states.values())


def test_ready_queue_priority() -> None:
    edges = {"x": (), "y": ("x",), "z": ("x",), "v": (), "u": ("v",), "w": ()}
    result, _ = simulate(edges, dict.fromkeys(edges, 1), workers=1)
    assert [u for _, u in starts(result)] == ["x", "v", "u", "w", "y", "z"]


def test_failure_skips_descendants_with_the_failed_upstream() -> None:
    edges = {"a": (), "b": ("a",), "c": ("b",), "d": (), "e": ("c", "d")}
    result, sink = simulate(edges, dict.fromkeys(edges, 1), frozenset({"b"}), workers=2)
    assert result.states["a"] is S.SUCCEEDED
    assert result.states["b"] is S.FAILED
    assert result.states["d"] is S.SUCCEEDED
    assert result.skipped == {"c": ("upstream_failed", "b"), "e": ("upstream_failed", "b")}
    skips = [(e.unit_id, e.data) for e in sink.events if e.type == "unit.skipped"]
    assert skips == [
        ("c", {"reason": "upstream_failed", "upstream": "b"}),
        ("e", {"reason": "upstream_failed", "upstream": "b"}),
    ]


def test_fail_fast_skips_everything_not_started() -> None:
    edges = {"a": (), "b": (), "c": (), "d": ("c",)}
    result, _ = simulate(edges, dict.fromkeys(edges, 1), frozenset({"c"}), fail_fast=True)
    # c starts first (it has a descendant), so it fails first
    assert result.states == {"c": S.FAILED, "d": S.SKIPPED, "a": S.SKIPPED, "b": S.SKIPPED}
    assert result.skipped["d"] == ("upstream_failed", "c")
    assert result.skipped["a"] == ("fail_fast", None)


def test_queued_events_follow_readiness() -> None:
    edges = {"a": (), "b": ("a",)}
    _, sink = simulate(edges, {"a": 1, "b": 1})
    assert [(e.type, e.unit_id, e.data) for e in sink.events] == [
        ("unit.queued", "a", {"downstream": 1}),
        ("unit.queued", "b", {"downstream": 0}),
    ]


def test_parents_outside_the_selection_are_satisfied() -> None:
    edges = {"a": (), "b": ("a",), "c": ("b",)}
    result, _ = simulate(edges, dict.fromkeys(edges, 1), selected=("b", "c"))
    assert set(result.states) == {"b", "c"}
    assert starts(result)[0] == (0.0, "b")


def test_from_plan_uses_the_plan_units() -> None:
    units = {
        "a": ExecutionUnit(unit_id="a", nodes=("m.a",), parents=()),
        "b": ExecutionUnit(unit_id="b", nodes=("m.b",), parents=("a",)),
    }
    plan = Plan.model_construct(units=units)
    graph = UnitGraph.from_plan(plan, ["b"])
    assert graph.units == ("b",)
    assert graph.parents == {"b": ()}
    with pytest.raises(ValueError, match="unknown"):
        UnitGraph.from_plan(plan, ["zz"])


def test_cycle_is_rejected() -> None:
    with pytest.raises(ValueError, match="cycle"):
        UnitGraph.from_parents({"a": ("b",), "b": ("a",)}, ["a", "b"])


def test_downstream_counts_are_transitive() -> None:
    graph = UnitGraph.from_parents(
        {"a": (), "b": ("a",), "c": ("a",), "d": ("b", "c"), "e": ()}, ["a", "b", "c", "d", "e"]
    )
    assert graph.downstream_counts() == {"a": 3, "b": 1, "c": 1, "d": 0, "e": 0}


def test_request_stop_cancels_what_has_not_started() -> None:
    clock = FakeClock(step=timedelta(0))
    sink = RecordingSink()
    bus = EventBus([(sink, 5)], clock=clock, invocation_id="inv")
    graph = UnitGraph.from_parents({"a": (), "b": ("a",), "c": ()}, ["a", "b", "c"])
    holder: list[Scheduler] = []

    def run_unit(h: UnitHandle, queued_at: float, token: CancelToken) -> UnitCompletion:
        holder[0].request_stop()
        return UnitCompletion(h, "succeeded", h.attempt)

    executor = ThreadExecutor(run_unit)
    holder.append(Scheduler(graph, executor, workers=1, fail_fast=False, bus=bus, clock=clock))
    result = holder[0].run()
    executor.shutdown(wait=True)
    assert result.states == {"a": S.SUCCEEDED, "c": S.CANCELLED, "b": S.CANCELLED}
    assert sink.types().count("unit.cancelled") == 2


def test_internal_error_drains_then_raises() -> None:
    clock = FakeClock(step=timedelta(0))
    bus = EventBus([(RecordingSink(), 5)], clock=clock, invocation_id="inv")
    graph = UnitGraph.from_parents({"a": (), "b": ("a",), "c": ()}, ["a", "b", "c"])

    def run_unit(h: UnitHandle, queued_at: float, token: CancelToken) -> UnitCompletion:
        if h.unit_id == "a":
            raise KeyError("bug")
        return UnitCompletion(h, "succeeded", h.attempt)

    executor = ThreadExecutor(run_unit)
    sched = Scheduler(graph, executor, workers=1, fail_fast=False, bus=bus, clock=clock)
    with pytest.raises(KeyError):
        sched.run()
    executor.shutdown(wait=True)
    assert sched.result is not None
    assert sched.result.states == {"a": S.FAILED, "c": S.CANCELLED, "b": S.SKIPPED}


def test_thread_executor_runs_every_unit() -> None:
    clock = FakeClock(step=timedelta(0))
    bus = EventBus([(RecordingSink(), 5)], clock=clock, invocation_id="inv")
    edges = {f"u{i}": (f"u{i - 1}",) if i % 3 else () for i in range(12)}
    graph = UnitGraph.from_parents(edges, edges)
    seen: list[str] = []
    executor = ThreadExecutor(
        lambda h, q, t: (seen.append(h.unit_id), UnitCompletion(h, "succeeded", h.attempt))[1]
    )
    result = Scheduler(graph, executor, workers=3, fail_fast=False, bus=bus, clock=clock).run()
    executor.shutdown(wait=True)
    assert sorted(seen) == sorted(edges)
    assert set(result.states.values()) == {S.SUCCEEDED}


def test_workers_must_be_positive() -> None:
    clock = FakeClock()
    bus = EventBus([], clock=clock, invocation_id="inv")
    graph = UnitGraph.from_parents({"a": ()}, ["a"])
    with pytest.raises(ValueError):
        Scheduler(
            graph,
            SimExecutor({}, clock),
            workers=0,
            fail_fast=False,
            bus=bus,
            clock=clock,
        )


# --- the state machine -------------------------------------------------------------------


def test_transition_table_covers_section_8_and_the_queued_exits() -> None:
    assert S.SKIPPED in TRANSITIONS[S.QUEUED]
    assert S.CANCELLED in TRANSITIONS[S.QUEUED]
    assert TRANSITIONS[S.RETRYING] == {S.RUNNING, S.CANCELLED}
    assert TERMINAL == {S.SUCCEEDED, S.FAILED, S.TIMED_OUT, S.CANCELLED, S.SKIPPED}


@pytest.mark.parametrize(("old", "new"), list(itertools.product(S, S)))
def test_check_transition_matches_the_table(old: UnitState, new: UnitState) -> None:
    if new in TRANSITIONS[old]:
        check_transition(old, new)
    else:
        with pytest.raises(IllegalTransition):
            check_transition(old, new)


# --- properties --------------------------------------------------------------------------


@st.composite
def scenarios(
    draw: st.DrawFn,
) -> tuple[dict[str, tuple[str, ...]], dict[str, int], frozenset[str], int]:
    n = draw(st.integers(1, 30))
    names = [f"u{i:02d}" for i in range(n)]
    edges = {
        names[j]: tuple(
            names[i] for i in sorted(draw(st.sets(st.sampled_from(range(j)), max_size=4)))
        )
        if j
        else ()
        for j in range(n)
    }
    durations = {u: draw(st.integers(1, 10)) for u in names}
    failures = frozenset(draw(st.sets(st.sampled_from(names))))
    return edges, durations, failures, draw(st.integers(1, 4))


def ancestors_failed(
    edges: Mapping[str, tuple[str, ...]], failures: frozenset[str]
) -> dict[str, S]:
    expected: dict[str, S] = {}
    for u in edges:  # names are topologically ordered by construction
        if any(expected[p] in (S.FAILED, S.SKIPPED) for p in edges[u]):
            expected[u] = S.SKIPPED
        else:
            expected[u] = S.FAILED if u in failures else S.SUCCEEDED
    return expected


@given(scenarios(), st.booleans())
def test_scheduler_properties(
    scenario: tuple[dict[str, tuple[str, ...]], dict[str, int], frozenset[str], int],
    fail_fast: bool,
) -> None:
    edges, durations, failures, workers = scenario
    result, sink = simulate(edges, durations, failures, workers=workers, fail_fast=fail_fast)

    # each unit ends in exactly one terminal state, and every step was a legal transition
    state = dict.fromkeys(edges, S.PENDING)
    terminal_entries: dict[str, int] = {}
    for _, u, new in result.trace:
        check_transition(state[u], new)
        state[u] = new
        if new in TERMINAL:
            terminal_entries[u] = terminal_entries.get(u, 0) + 1
    assert state == result.states
    assert terminal_entries == dict.fromkeys(edges, 1)

    # a unit starts only after all its parents succeeded
    succeeded: set[str] = set()
    running = 0
    failed_yet = False
    for _, u, new in result.trace:
        if new is S.RUNNING:
            assert set(edges[u]) <= succeeded
            running += 1
            assert running <= workers  # never more than `workers` at once
            assert not (fail_fast and failed_yet)  # no start after the first failure
        elif new in (S.SUCCEEDED, S.FAILED):
            running -= 1
            failed_yet = failed_yet or new is S.FAILED
            if new is S.SUCCEEDED:
                succeeded.add(u)

    if not fail_fast:  # skipped iff a selected ancestor failed
        assert result.states == ancestors_failed(edges, failures)

    # work conservation, derived from the trace alone: once all entries at an instant are
    # applied, a unit whose parents all succeeded and that has not started means all workers
    # are busy
    live = dict.fromkeys(edges, S.PENDING)
    for i, (t, u, new) in enumerate(result.trace):
        live[u] = new
        if i + 1 < len(result.trace) and result.trace[i + 1][0] == t:
            continue
        ready = [
            v
            for v, s in live.items()
            if s in (S.PENDING, S.QUEUED) and all(live[p] is S.SUCCEEDED for p in edges[v])
        ]
        if ready:
            assert sum(s is S.RUNNING for s in live.values()) == workers

    # skip reasons and upstream
    events = {e.unit_id: e.data for e in sink.events if e.type == "unit.skipped"}
    assert events == {u: {"reason": r, "upstream": up} for u, (r, up) in result.skipped.items()}
    assert set(result.skipped) == {u for u, s in result.states.items() if s is S.SKIPPED}
    ancestors: dict[str, set[str]] = {}
    for u in edges:
        ancestors[u] = set(edges[u]).union(*(ancestors[p] for p in edges[u]))
    at = {(u, st): i for i, (_, u, st) in enumerate(result.trace)}
    for u, (reason, upstream) in result.skipped.items():
        failed_anc = {a for a in ancestors[u] if result.states[a] is S.FAILED}
        if reason == "upstream_failed":
            assert upstream in failed_anc
        else:
            assert fail_fast
            assert upstream is None
            # a failed ancestor is only possible if it was still running at the sweep
            assert all(at[(a, S.FAILED)] > at[(u, S.SKIPPED)] for a in failed_anc)
    for u in edges:
        if any(result.states[a] is S.FAILED for a in ancestors[u]):
            assert result.states[u] is S.SKIPPED
            if result.skipped[u][0] == "fail_fast":
                assert fail_fast

    # determinism
    again, sink2 = simulate(edges, durations, failures, workers=workers, fail_fast=fail_fast)
    assert stream(sink) == stream(sink2)
    assert again.trace == result.trace
