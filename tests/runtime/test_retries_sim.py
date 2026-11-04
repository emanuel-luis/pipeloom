"""Retries, timeouts and stops on the simulated executor, with a model of the expected outcome."""

from __future__ import annotations

import itertools
from collections.abc import Mapping, Sequence
from datetime import timedelta

from hypothesis import given
from hypothesis import strategies as st

from pipeloom.events.bus import EventBus
from pipeloom.runtime.control import RunControl
from pipeloom.runtime.retry import BACKOFF_CAP, UnitPolicy
from pipeloom.runtime.scheduler import Scheduler, ScheduleResult, UnitGraph
from pipeloom.runtime.states import TERMINAL, UnitState, check_transition
from pipeloom.testing import FakeClock, RecordingSink
from pipeloom.testing.sim import SimAttempt, SimExecutor

S = UnitState
EPS = 3e-6
Scenario = tuple[
    dict[str, tuple[str, ...]],
    dict[str, list[SimAttempt]],
    dict[str, UnitPolicy],
    int,
]


def simulate(
    edges: Mapping[str, tuple[str, ...]],
    script: Mapping[str, Sequence[SimAttempt]],
    policies: Mapping[str, UnitPolicy],
    *,
    workers: int = 1,
    fail_fast: bool = False,
    control: RunControl | None = None,
    stop_at: int | None = None,
    cancel_latency: int = 0,
    cancel_grace: float = 30.0,
) -> tuple[ScheduleResult, RecordingSink]:
    clock = FakeClock(step=timedelta(0))
    sink = RecordingSink()
    bus = EventBus([(sink, 5)], clock=clock, invocation_id="inv")
    ctl = control or RunControl()
    executor = SimExecutor(
        script, clock, cancel_latency=cancel_latency, control=ctl, stop_at=stop_at
    )
    sched = Scheduler(
        UnitGraph.from_parents(edges, edges),
        executor,
        workers=workers,
        fail_fast=fail_fast,
        bus=bus,
        clock=clock,
        policies=policies,
        control=ctl,
        cancel_grace=cancel_grace,
        run_id="run",
        poll_interval=10_000.0,
    )
    return sched.run(), sink


def expected_outcome(policy: UnitPolicy, script: Sequence[SimAttempt]) -> tuple[int, S]:
    attempts = used = 0
    while True:
        entry = script[min(attempts, len(script) - 1)]
        attempts += 1
        if policy.timeout is not None and (
            entry.duration is None or entry.duration > policy.timeout
        ):
            return attempts, S.TIMED_OUT
        if entry.status == "succeeded":
            return attempts, S.SUCCEEDED
        category = entry.category or "permanent"
        if used < policy.retries and (policy.retry_on == "any" or category == "transient"):
            used += 1
            continue
        return attempts, S.FAILED


@st.composite
def scenarios(draw: st.DrawFn) -> Scenario:
    n = draw(st.integers(1, 8))
    names = [f"u{i}" for i in range(n)]
    edges = {
        names[j]: tuple(
            names[i] for i in sorted(draw(st.sets(st.sampled_from(range(j)), max_size=3)))
        )
        if j
        else ()
        for j in range(n)
    }
    script: dict[str, list[SimAttempt]] = {}
    policies: dict[str, UnitPolicy] = {}
    for u in names:
        timeout = draw(st.one_of(st.none(), st.integers(1, 6)))
        policies[u] = UnitPolicy(
            retries=draw(st.integers(0, 3)),
            retry_delay=float(draw(st.integers(0, 40))),
            retry_on=draw(st.sampled_from(["transient", "any"])),
            timeout=None if timeout is None else float(timeout),
        )
        durations = st.integers(1, 8)
        if timeout is not None:
            durations = st.one_of(st.none(), durations)
        script[u] = draw(
            st.lists(
                st.builds(
                    SimAttempt,
                    duration=durations,
                    status=st.sampled_from(["succeeded", "failed"]),
                    category=st.sampled_from(["transient", "permanent", None]),
                ),
                min_size=1,
                max_size=4,
            )
        )
    return edges, script, policies, draw(st.integers(1, 3))


def stream(sink: RecordingSink) -> list[tuple[int, str, str | None, int | None, str]]:
    return [(e.seq, e.type, e.unit_id, e.attempt, str(sorted(e.data.items()))) for e in sink.events]


@given(scenarios())
def test_retry_and_timeout_properties(scenario: Scenario) -> None:
    edges, script, policies, workers = scenario
    result, sink = simulate(edges, script, policies, workers=workers)

    # the transition checker never raises on the trace, and every unit ends exactly once
    state = dict.fromkeys(edges, S.PENDING)
    terminal = dict.fromkeys(edges, 0)
    for _, u, new in result.trace:
        check_transition(state[u], new)
        state[u] = new
        terminal[u] += new in TERMINAL
    assert state == result.states
    assert set(terminal.values()) == {1}

    # the outcome of every unit follows the model: attempts, retries and the final state
    want: dict[str, S] = {}
    for u in edges:  # names are topologically ordered
        if any(want[p] is not S.SUCCEEDED for p in edges[u]):
            want[u] = S.SKIPPED
            continue
        attempts, final = expected_outcome(policies[u], script[u])
        want[u] = final
        runs = [t for t, v, s in result.trace if v == u and s is S.RUNNING]
        assert len(runs) == attempts <= policies[u].retries + 1 + 0 or final is S.TIMED_OUT
        assert len(runs) <= policies[u].retries + 1
    assert result.states == want

    # a retry starts no earlier than failure + d/2, and the logged delay is within [d/2, d] <= cap
    for u in edges:
        marks = [s for _, v, s in result.trace if v == u]
        for before, after in itertools.pairwise(marks):
            if before is S.RETRYING:
                assert after is S.RUNNING
    for e in sink.events:
        if e.type != "unit.retrying":
            continue
        assert e.unit_id is not None and e.attempt is not None
        base = policies[e.unit_id].retry_delay
        n = len(
            [
                x
                for x in sink.events
                if x.type == "unit.retrying" and x.unit_id == e.unit_id and x.seq <= e.seq
            ]
        )
        d = min(max(base, BACKOFF_CAP), base * 2 ** (n - 1))
        assert d / 2 * 1000 - 1 <= e.data["delay_ms"] <= d * 1000 + 1
        assert e.data["delay_ms"] <= max(base, BACKOFF_CAP) * 1000 + 1
        assert e.data["next_attempt"] == e.attempt + 1
    # ... and the restart time honours that delay
    last_fail: dict[str, float] = {}
    delays = {
        (e.unit_id, e.attempt): e.data["delay_ms"] / 1000
        for e in sink.events
        if e.type == "unit.retrying"
    }
    attempt_no = dict.fromkeys(edges, 0)
    for t, u, s in result.trace:
        if s is S.RUNNING:
            attempt_no[u] += 1
            if u in last_fail:
                assert t - last_fail.pop(u) >= delays[(u, attempt_no[u] - 1)] - EPS
        elif s is S.RETRYING:
            last_fail[u] = t

    # a unit that times out ends exactly at its deadline, and its descendants are skipped
    start: dict[str, float] = {}
    for t, u, s in result.trace:
        if s is S.RUNNING:
            start[u] = t
        elif s is S.TIMED_OUT:
            assert abs(t - (start[u] + (policies[u].timeout or 0))) < EPS
    for u, s in result.states.items():
        if s is S.TIMED_OUT:
            for v in edges:
                if u in edges[v]:
                    assert result.states[v] is S.SKIPPED

    # never more live units than workers (a retrying unit holds no slot)
    live = 0
    seen = dict.fromkeys(edges, S.PENDING)
    for _, u, s in result.trace:
        if s is S.RUNNING:
            live += 1
            assert live <= workers
        elif seen[u] is S.RUNNING:
            live -= 1
        seen[u] = s

    # the same script gives the same events and trace
    again, sink2 = simulate(edges, script, policies, workers=workers)
    assert again.trace == result.trace and stream(sink) == stream(sink2)


def test_a_transient_failure_retries_and_a_permanent_one_does_not() -> None:
    edges = {"a": (), "b": ()}
    script = {
        "a": [SimAttempt(1, "failed", "transient"), SimAttempt(1, "succeeded")],
        "b": [SimAttempt(1, "failed", "permanent"), SimAttempt(1, "succeeded")],
    }
    policy = UnitPolicy(2, 4.0, "transient", None)
    result, sink = simulate(edges, script, {"a": policy, "b": policy}, workers=2)
    assert result.states == {"a": S.SUCCEEDED, "b": S.FAILED}
    assert [(e.unit_id, e.attempt) for e in sink.events if e.type == "unit.retrying"] == [("a", 1)]
    any_policy = UnitPolicy(2, 4.0, "any", None)
    result, _ = simulate(edges, script, {"a": any_policy, "b": any_policy}, workers=2)
    assert result.states == {"a": S.SUCCEEDED, "b": S.SUCCEEDED}


def test_a_hanging_unit_times_out_at_its_deadline_and_is_not_retried() -> None:
    edges = {"a": (), "b": ("a",)}
    script = {"a": [SimAttempt(None, "succeeded")], "b": [SimAttempt(1, "succeeded")]}
    policies = {"a": UnitPolicy(3, 1.0, "any", 5.0), "b": UnitPolicy(0, 0.0, "transient", None)}
    result, sink = simulate(edges, script, policies)
    assert result.states == {"a": S.TIMED_OUT, "b": S.SKIPPED}
    assert (5.0, "a", S.TIMED_OUT) in result.trace
    assert result.skipped == {"b": ("upstream_failed", "a")}
    timed = [e for e in sink.events if e.type == "unit.timed_out"]
    assert [(e.unit_id, e.attempt, e.data["timeout_s"]) for e in timed] == [("a", 1, 5.0)]
    assert result.completions["a"].result is not None
    assert result.completions["a"].result.error is not None
    assert result.completions["a"].result.error.code == "PLX102"


def test_fail_fast_cancels_a_retrying_unit() -> None:
    edges = {"a": (), "b": ()}
    script = {
        "a": [SimAttempt(1, "failed", "transient"), SimAttempt(1, "succeeded")],
        "b": [SimAttempt(2, "failed", "permanent")],
    }
    policies = {
        "a": UnitPolicy(1, 100.0, "transient", None),
        "b": UnitPolicy(0, 0.0, "transient", None),
    }
    result, _ = simulate(edges, script, policies, workers=2, fail_fast=True)
    assert result.states == {"a": S.CANCELLED, "b": S.FAILED}
    assert [s for _, u, s in result.trace if u == "a"] == [
        S.QUEUED,
        S.RUNNING,
        S.RETRYING,
        S.CANCELLED,
    ]


def test_a_stop_cancels_waiting_units_and_running_units_that_cannot_finish_in_the_grace() -> None:
    edges = {"a": (), "b": ("a",), "c": ()}
    script = {
        "a": [SimAttempt(100, "succeeded")],
        "b": [SimAttempt(1, "succeeded")],
        "c": [SimAttempt(2, "succeeded")],
    }
    policies = dict.fromkeys(edges, UnitPolicy(0, 0.0, "transient", None))
    control = RunControl()
    result, _ = simulate(
        edges,
        script,
        policies,
        workers=2,
        control=control,
        stop_at=5,
        cancel_latency=50,
        cancel_grace=10.0,
    )
    assert result.states == {"a": S.CANCELLED, "b": S.CANCELLED, "c": S.SUCCEEDED}
    assert (15.0, "a", S.CANCELLED) in result.trace  # the grace ran out
    assert not [t for t, u, s in result.trace if s is S.RUNNING and t > 5]  # nothing starts after
    assert control.stop_requested


def test_a_unit_that_finishes_within_the_grace_keeps_its_result() -> None:
    edges = {"a": ()}
    script = {"a": [SimAttempt(8, "succeeded")]}
    policies = {"a": UnitPolicy(0, 0.0, "transient", None)}
    result, _ = simulate(
        edges, script, policies, control=RunControl(), stop_at=5, cancel_latency=50, cancel_grace=10
    )
    assert result.states == {"a": S.SUCCEEDED}


def test_a_hard_stop_skips_the_grace() -> None:
    edges = {"a": ()}
    script = {"a": [SimAttempt(100, "succeeded")]}
    policies = {"a": UnitPolicy(0, 0.0, "transient", None)}
    control = RunControl()
    control.request_stop(hard=True)
    result, _ = simulate(edges, script, policies, control=control, cancel_latency=50)
    assert result.states == {"a": S.CANCELLED}
    assert result.trace[-1][0] == 0.0
