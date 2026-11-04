"""Retries and timeouts with real threads and real child processes."""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from typing import Any

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.graph.selectors import Selection
from pipeloom.platform import SystemClock
from pipeloom.runtime.control import CancelToken, RunControl
from pipeloom.runtime.executors import UnitCompletion, UnitHandle
from pipeloom.runtime.executors.process import ProcessExecutor
from pipeloom.runtime.executors.thread import ThreadExecutor
from pipeloom.runtime.retry import UnitPolicy
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.runtime.scheduler import Scheduler, UnitGraph
from pipeloom.runtime.states import UnitState
from pipeloom.testing import RecordingSink
from tests.conftest import make_project
from tests.runtime import test_process
from tests.runtime.test_runner import results, run_project

HEAD = "config_version: 1\nname: p\npython_paths: [src]\ntargets:\n  ci:\n"
ENGINE = "    engine: {kind: memory}\n    catalog: {kind: memory}\n"
FLAKY = """from pipeloom.engine.errors import TransientError


def flaky(ctx):
    if ctx.attempt == 1:
        raise TransientError("try again")
    return ({"x": 1},)


def broken(ctx):
    if ctx.attempt == 1:
        raise ValueError("deterministic")
    return ({"x": 1},)
"""


def flaky_project(tmp_path: Path, cfg: str, fn: str = "flaky") -> Path:
    models = f'models:\n  - {{name: a, python: "m:{fn}", {cfg}}}\n'
    return make_project(
        tmp_path, {"pipeloom.yml": HEAD + ENGINE, "models/m.yml": models, "src/m.py": FLAKY}
    )


def attempts(root: Path, unit: str = "a") -> list[dict[str, Any]]:
    folder = root / "target" / "runs" / "run1" / "units" / unit
    return [json.loads(p.read_bytes()) for p in sorted(folder.glob("attempt-*.json"))]


def run_real_clock(root: Path) -> tuple[RunOutcome, RecordingSink]:
    """Like `run_project`, but time really passes: a timeout needs a clock that moves."""
    project = Project(ProjectOptions(project_dir=root, target="ci", env={"PIPELOOM_USER": "t"}))
    sink = RecordingSink()
    with EventBus([(sink, 5)], clock=SystemClock(), invocation_id="inv") as bus:
        return project.run(RunOptions(run_id="run1", workers=1), bus), sink


# -- the thread executor ----------------------------------------------------------------------


class RecordingSession:
    def __init__(self) -> None:
        self.cancelled: list[str] = []

    def cancel(self, unit_id: str) -> None:
        self.cancelled.append(unit_id)


def test_a_blocked_thread_times_out_once_and_its_late_result_is_dropped() -> None:
    release = threading.Event()
    finished = threading.Event()
    interrupts: list[tuple[str, str]] = []
    session = RecordingSession()

    def run_unit(h: UnitHandle, queued_at: float, token: CancelToken) -> UnitCompletion:
        release.wait(10)
        finished.set()
        return UnitCompletion(h, "succeeded", h.attempt)

    executor = ThreadExecutor(
        run_unit,
        session=session,  # type: ignore[arg-type]
        on_interrupt=lambda h, reason: interrupts.append((h.unit_id, reason)),
    )
    sink = RecordingSink()
    clock = SystemClock()
    bus = EventBus([(sink, 5)], clock=clock, invocation_id="inv")
    graph = UnitGraph.from_parents({"a": (), "b": ("a",)}, ["a", "b"])
    policies = {"a": UnitPolicy(2, 0.0, "any", 0.3), "b": UnitPolicy(0, 0.0, "transient", None)}
    started = time.monotonic()
    result = Scheduler(
        graph, executor, workers=1, fail_fast=False, bus=bus, clock=clock, policies=policies
    ).run()
    assert time.monotonic() - started < 3
    assert result.states == {"a": UnitState.TIMED_OUT, "b": UnitState.SKIPPED}
    assert session.cancelled == ["a"]  # once
    assert interrupts == [("a", "timeout")]
    assert executor.abandoned_threads == 1
    release.set()
    assert finished.wait(5)
    executor.shutdown(wait=True)
    assert executor.wait_any(0.05) is None  # the late completion was dropped
    assert [e.type for e in sink.events if e.type.startswith("unit.")] == [
        "unit.queued",
        "unit.timed_out",
        "unit.skipped",
    ]


def test_a_timed_out_attempt_file_survives_the_late_thread(tmp_path: Path) -> None:
    src = "import time\n\n\ndef hang(ctx):\n    time.sleep(1.8)\n    return ({'x': 1},)\n"
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": HEAD + ENGINE,
            "models/m.yml": 'models:\n  - {name: a, python: "m:hang", timeout: 1, retries: 2}\n',
            "src/m.py": src,
        },
    )
    outcome, sink = run_real_clock(root)
    assert (outcome.status, outcome.exit_code) == ("failed", 1)
    assert [d.code for d in outcome.diagnostics] == ["PLX102"]
    (row,) = results(root)["results"]
    assert (row["status"], row["error"]["code"]) == ("timed_out", "PLX102")
    assert results(root)["run"]["counts"]["timed_out"] == 1
    done = next(e for e in sink.events if e.type == "run.completed")
    assert done.data["abandoned_threads"] == 1
    assert [e.attempt for e in sink.events if e.type == "unit.timed_out"] == [1]
    assert not [e for e in sink.events if e.type == "unit.retrying"]  # a timeout is not retried
    time.sleep(1.5)  # the thread wakes up and tries to write its own result
    (late,) = attempts(root)
    assert late["status"] == "timed_out"
    assert len(list((root / "target/runs/run1/units/a").glob("attempt-*.json"))) == 1


# -- retries through Project.run -------------------------------------------------------------


def test_a_transient_failure_is_retried_with_its_own_attempt_file(tmp_path: Path) -> None:
    root = flaky_project(tmp_path, "retries: 2, retry_delay: 0")
    outcome, sink = run_project(root)
    assert outcome.exit_code == 0
    rows = attempts(root)
    assert [(r["attempt"], r["status"]) for r in rows] == [(1, "failed"), (2, "succeeded")]
    assert rows[0]["error"]["category"] == "transient"
    assert rows[1]["retries_used"] == 1
    retrying = [e for e in sink.events if e.type == "unit.retrying"]
    assert [(e.attempt, e.data["next_attempt"], e.data["delay_ms"]) for e in retrying] == [
        (1, 2, 0)
    ]
    assert results(root)["run"]["status"] == "succeeded"


def test_a_permanent_failure_is_not_retried_unless_retry_on_is_any(tmp_path: Path) -> None:
    root = flaky_project(tmp_path / "t", "retries: 2, retry_delay: 0", "broken")
    outcome, _ = run_project(root)
    assert outcome.exit_code == 1 and len(attempts(root)) == 1
    root = flaky_project(tmp_path / "any", "retries: 2, retry_delay: 0, retry_on: any", "broken")
    outcome, _ = run_project(root)
    assert outcome.exit_code == 0
    assert [r["status"] for r in attempts(root)] == ["failed", "succeeded"]


def test_retries_are_used_up(tmp_path: Path) -> None:
    src = "def always(ctx):\n    raise ConnectionError('down')\n"
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": HEAD + ENGINE,
            "models/m.yml": 'models:\n  - {name: a, python: "m:always", retries: 1, retry_delay: 0}\n',
            "src/m.py": src,
        },
    )
    outcome, _ = run_project(root)
    assert outcome.exit_code == 1
    assert [r["status"] for r in attempts(root)] == ["failed", "failed"]


# -- the process executor --------------------------------------------------------------------

SLEEP = "def slow(ctx):\n    import time\n\n    time.sleep(60)\n"
MODELS = (
    "models:\n"
    '  - {name: slow, python: "m:slow", timeout: 1}\n'
    "  - {name: after, sql: select * from slow, inputs: {slow: {ref: slow}}}\n"
)


def test_a_sleeping_child_is_stopped_and_leaves_no_orphan(tmp_path: Path, monkeypatch: Any) -> None:
    made: list[ProcessExecutor] = []
    real = ProcessExecutor.__init__

    def spy(self: ProcessExecutor, **kwargs: Any) -> None:
        real(self, **kwargs)
        made.append(self)

    monkeypatch.setattr(ProcessExecutor, "__init__", spy)
    root = test_process.project_dir(tmp_path, MODELS, SLEEP)
    started = time.monotonic()
    outcome, sink = test_process.run(root, RunOptions(run_id="r1", workers=1, cancel_grace=1.0))
    assert time.monotonic() - started < 10
    assert outcome.exit_code == 1
    doc = test_process.results(root, "r1")
    assert {r["unit_id"]: r["status"] for r in doc["results"]} == {
        "after": "skipped",
        "slow": "timed_out",
    }
    assert [d.code for d in outcome.diagnostics if d.code.startswith("PLX")] == ["PLX102"]
    assert [e.unit_id for e in sink.events if e.type == "unit.timed_out"] == ["slow"]  # one
    assert made and all(p.poll() is not None for p in made[0].popens)


TRANSIENT = (
    "from pipeloom.engine.errors import TransientError\n\n\n"
    "def flaky(ctx):\n    if ctx.attempt == 1:\n        raise TransientError('again')\n"
    "    return ({'x': 1},)\n"
)


def test_the_parent_retries_a_transient_child_failure(tmp_path: Path) -> None:
    models = 'models:\n  - {name: a, python: "m:flaky", retries: 1, retry_delay: 0}\n'
    root = test_process.project_dir(tmp_path, models, TRANSIENT)
    outcome, sink = test_process.run(root, RunOptions(run_id="r1", workers=1))
    assert outcome.exit_code == 0
    folder = root / "target/runs/r1/units/a"
    rows = [json.loads(p.read_bytes()) for p in sorted(folder.glob("attempt-*.json"))]
    assert [r["status"] for r in rows] == ["failed", "succeeded"]
    assert [e.attempt for e in sink.events if e.type == "unit.retrying"] == [1]


def test_unit_mode_never_retries(tmp_path: Path) -> None:
    models = 'models:\n  - {name: a, python: "m:flaky", retries: 3, retry_delay: 0}\n'
    root = test_process.project_dir(tmp_path, models, TRANSIENT)
    outcome, _ = test_process.run(root, RunOptions(Selection(("unit:a",)), run_id="u1"))
    assert outcome.exit_code == 1
    folder = root / "target/runs/u1/units/a"
    assert len(list(folder.glob("attempt-*.json"))) == 1


def test_a_unit_that_finishes_at_its_deadline_is_not_also_timed_out() -> None:
    """The runner passed its point of no return before the deadline fired."""
    gate = threading.Event()

    def run_unit(h: UnitHandle, queued_at: float, token: CancelToken) -> UnitCompletion:
        assert token.finish()  # it has its result and emitted its own terminal event
        gate.wait(5)
        return UnitCompletion(h, "succeeded", h.attempt)

    executor = ThreadExecutor(run_unit)
    threading.Timer(0.6, gate.set).start()
    sink = RecordingSink()
    clock = SystemClock()
    bus = EventBus([(sink, 5)], clock=clock, invocation_id="inv")
    graph = UnitGraph.from_parents({"a": ()}, ["a"])
    policies = {"a": UnitPolicy(0, 0.0, "transient", 0.2)}
    result = Scheduler(
        graph, executor, workers=1, fail_fast=False, bus=bus, clock=clock, policies=policies
    ).run()
    executor.shutdown(wait=True)
    assert result.states == {"a": UnitState.SUCCEEDED}
    assert not [e for e in sink.events if e.type == "unit.timed_out"]


def test_waiting_for_a_finished_unit_does_not_spin() -> None:
    gate = threading.Event()
    calls: list[float | None] = []

    def run_unit(h: UnitHandle, queued_at: float, token: CancelToken) -> UnitCompletion:
        assert token.finish()
        gate.wait(5)
        return UnitCompletion(h, "succeeded", h.attempt)

    executor = ThreadExecutor(run_unit)
    real = executor.wait_any

    def counting(timeout: float | None) -> UnitCompletion | None:
        calls.append(timeout)
        return real(timeout)

    executor.wait_any = counting  # type: ignore[method-assign]
    threading.Timer(0.8, gate.set).start()
    clock = SystemClock()
    bus = EventBus([], clock=clock, invocation_id="inv")
    policies = {"a": UnitPolicy(0, 0.0, "transient", 0.1)}
    Scheduler(
        UnitGraph.from_parents({"a": ()}, ["a"]),
        executor,
        workers=1,
        fail_fast=False,
        bus=bus,
        clock=clock,
        policies=policies,
    ).run()
    assert len(calls) < 40  # a poll interval, not a busy loop


def test_a_stop_grace_that_ends_after_the_runner_finished_keeps_its_result() -> None:
    gate = threading.Event()

    def run_unit(h: UnitHandle, queued_at: float, token: CancelToken) -> UnitCompletion:
        assert token.finish()
        gate.wait(5)
        return UnitCompletion(h, "succeeded", h.attempt)

    executor = ThreadExecutor(run_unit)
    control = RunControl()
    threading.Timer(0.1, control.request_stop).start()
    threading.Timer(0.7, gate.set).start()
    sink = RecordingSink()
    clock = SystemClock()
    bus = EventBus([(sink, 5)], clock=clock, invocation_id="inv")
    result = Scheduler(
        UnitGraph.from_parents({"a": ()}, ["a"]),
        executor,
        workers=1,
        fail_fast=False,
        bus=bus,
        clock=clock,
        control=control,
        cancel_grace=0.3,
    ).run()
    assert result.states == {"a": UnitState.SUCCEEDED}
    assert not [e for e in sink.events if e.type == "unit.cancelled"]


STUBBORN = """import subprocess
import sys
import threading
import time
from pathlib import Path


def stubborn(ctx):
    beat = Path(__file__).parent / "beat.txt"
    code = (
        "import time, pathlib; p = pathlib.Path(" + repr(str(beat)) + "); "
        "exec('while True: p.write_text(str(time.time())); time.sleep(0.1)')"
    )
    subprocess.Popen([sys.executable, "-c", code])  # a grandchild that ignores every stop
    threading.Thread(target=time.sleep, args=(120,)).start()  # keeps the child alive
    time.sleep(120)
"""


def test_a_grandchild_does_not_survive_or_hold_up_the_run(tmp_path: Path) -> None:
    models = 'models:\n  - {name: slow, python: "m:stubborn", timeout: 1}\n'
    root = test_process.project_dir(tmp_path, models, STUBBORN)
    started = time.monotonic()
    outcome, _ = test_process.run(root, RunOptions(run_id="r1", workers=1, cancel_grace=1.0))
    assert time.monotonic() - started < 20
    assert outcome.exit_code == 1, outcome.diagnostics
    assert outcome.run_results is not None, outcome.diagnostics
    assert {r["status"] for r in test_process.results(root, "r1")["results"]} == {"timed_out"}
    if sys.platform == "win32":
        return  # no job objects yet: a grandchild of a child that exits on its own survives
    beat = root / "src" / "beat.txt"
    time.sleep(0.5)  # a live grandchild would have written by now
    before = beat.read_text(encoding="utf-8") if beat.exists() else ""
    time.sleep(0.5)
    assert (beat.read_text(encoding="utf-8") if beat.exists() else "") == before


def test_a_final_cancel_after_the_runner_passed_its_checkpoint_is_not_a_timeout() -> None:
    token = CancelToken()
    assert token.set("cancel", final=True)
    assert not token.finish()
    assert token.final_reason == "cancel"
    other = CancelToken()
    assert other.finish()
    assert not other.set("cancel", final=True)  # too late: the runner has its result
    assert other.final_reason is None
