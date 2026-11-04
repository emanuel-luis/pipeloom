"""Stops: through RunControl on the memory engine, on the simulator, by signal and across processes."""

from __future__ import annotations

import builtins
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._signals import install_stop_handlers
from pipeloom.events.bus import EventBus
from pipeloom.platform import SystemClock
from pipeloom.runtime.control import RunControl
from pipeloom.runtime.executors.process import ProcessExecutor
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.runtime.states import UnitState
from pipeloom.testing import RecordingSink
from tests.conftest import make_project
from tests.runtime.test_process import PIPELOOM_YML as TASK_YML
from tests.runtime.test_retries_sim import Scenario, scenarios, simulate

HEAD = "config_version: 1\nname: p\npython_paths: [src]\ntargets:\n  ci:\n"
ENGINE = "    engine: {kind: memory}\n    catalog: {kind: memory}\n"
CHAIN = """models:
  - {name: a, python: "m:body"}
  - {name: b, sql: select * from a, inputs: {a: {ref: a}}}
  - {name: c, sql: select * from b, inputs: {b: {ref: b}}}
"""
SOFT = """import builtins


def body(ctx):
    builtins.PL_CONTROL.request_stop()
    return ({"x": 1},)
"""
HANG = """import builtins


def body(ctx):
    builtins.PL_CONTROL.request_stop(hard=builtins.PL_HARD)
    builtins.PL_RELEASE.wait(20)
    return ({"x": 1},)
"""


def chain(tmp_path: Path, src: str) -> Path:
    return make_project(
        tmp_path,
        {"pipeloom.yml": HEAD + ENGINE, "models/m.yml": CHAIN, "src/m.py": src},
    )


def run_stopped(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    hard: bool = False,
    grace: float | None = None,
) -> tuple[RunOutcome, RecordingSink, RunControl, float]:
    control = RunControl()
    release = threading.Event()
    monkeypatch.setattr(builtins, "PL_CONTROL", control, raising=False)
    monkeypatch.setattr(builtins, "PL_HARD", hard, raising=False)
    monkeypatch.setattr(builtins, "PL_RELEASE", release, raising=False)
    project = Project(ProjectOptions(project_dir=root, target="ci", env={"PIPELOOM_USER": "t"}))
    sink = RecordingSink()
    started = time.monotonic()
    try:
        with EventBus([(sink, 5)], clock=SystemClock(), invocation_id="inv") as bus:
            outcome = project.run(
                RunOptions(run_id="run1", workers=1, cancel_grace=grace), bus, control
            )
    finally:
        release.set()
    return outcome, sink, control, time.monotonic() - started


def statuses(root: Path, run_id: str = "run1") -> dict[str, str]:
    path = root / "target" / "runs" / run_id / "run_results.json"
    doc = json.loads(path.read_bytes().decode("utf-8"))
    assert doc["run"]["status"] == "cancelled"
    return {r["unit_id"]: r["status"] for r in doc["results"]}


# -- RunControl on the memory engine ----------------------------------------------------------


def test_a_stop_from_a_model_body_cancels_what_is_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = chain(tmp_path, SOFT)
    outcome, sink, control, _ = run_stopped(root, monkeypatch)
    assert (outcome.status, outcome.exit_code) == ("cancelled", 130), outcome.diagnostics
    found = statuses(root)
    assert found["a"] in ("succeeded", "cancelled")  # it finished within the grace, or was caught
    assert found["b"] == found["c"] == "cancelled"
    assert sink.types().count("run.cancel_requested") == 1
    event = next(e for e in sink.events if e.type == "run.cancel_requested")
    assert event.data == {"signal": None, "grace_s": 30.0}
    assert sink.events[-1].type == "run.completed" and sink.events[-1].data["status"] == "cancelled"
    assert control.stop_requested and not control.hard


def test_a_soft_stop_kills_a_stuck_unit_when_the_grace_runs_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = chain(tmp_path, HANG)
    outcome, sink, _, elapsed = run_stopped(root, monkeypatch, grace=1)
    assert 0.9 <= elapsed < 8
    assert statuses(root) == {"a": "cancelled", "b": "cancelled", "c": "cancelled"}
    assert "PLX104" in {d.code for d in outcome.diagnostics}
    assert sink.types().count("run.cancel_requested") == 1


def test_a_hard_stop_skips_the_grace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = chain(tmp_path, HANG)
    outcome, sink, control, elapsed = run_stopped(root, monkeypatch, hard=True, grace=30)
    assert elapsed < 8  # not 30
    assert control.hard and outcome.exit_code == 130
    assert statuses(root)["a"] == "cancelled"
    done = sink.events[-1]
    assert done.type == "run.completed" and done.data["abandoned_threads"] == 1


# -- the simulator ----------------------------------------------------------------------------


@given(scenarios(), st.integers(0, 20))
def test_nothing_starts_after_a_stop(scenario: Scenario, stop_at: int) -> None:
    edges, script, policies, workers = scenario
    control = RunControl()
    result, sink = simulate(
        edges, script, policies, workers=workers, control=control, stop_at=stop_at
    )
    started = [t for t, _, s in result.trace if s is UnitState.RUNNING]
    assert all(t <= stop_at for t in started) or not control.stop_requested
    assert sink.types().count("run.cancel_requested") == int(control.stop_requested)
    assert all(
        s
        in (
            UnitState.SUCCEEDED,
            UnitState.FAILED,
            UnitState.TIMED_OUT,
            UnitState.SKIPPED,
            UnitState.CANCELLED,
        )
        for s in result.states.values()
    )
    if control.stop_requested:
        stop = [t for t, _, s in result.trace if s is UnitState.RUNNING and t > stop_at]
        assert not stop


# -- the signal handlers ----------------------------------------------------------------------


def test_signals_stop_softly_then_hard_then_raise_and_handlers_come_back() -> None:
    before = signal.getsignal(signal.SIGINT)
    control = RunControl()
    with install_stop_handlers(control):
        signal.raise_signal(signal.SIGINT)
        assert control.stop_requested and not control.hard
        assert control.signal_name == "SIGINT"
        signal.raise_signal(signal.SIGINT)
        assert control.hard
        with pytest.raises(KeyboardInterrupt):
            signal.raise_signal(signal.SIGINT)
    assert signal.getsignal(signal.SIGINT) is before


def test_handlers_are_not_installed_off_the_main_thread() -> None:
    before = signal.getsignal(signal.SIGINT)
    seen: list[object] = []

    def work() -> None:
        with install_stop_handlers(RunControl()):
            seen.append(signal.getsignal(signal.SIGINT))

    thread = threading.Thread(target=work)
    thread.start()
    thread.join()
    assert seen == [before]


# -- real processes ---------------------------------------------------------------------------

SLOW = """import os
import time


def body(ctx):
    with open(os.environ["PL_MARK"], "w") as fh:
        fh.write("1")
    time.sleep(2)
    return ({"x": 1},)
"""


def has_console() -> bool:
    if sys.platform != "win32":
        return True
    import ctypes

    return bool(ctypes.windll.kernel32.GetConsoleWindow())  # type: ignore[attr-defined]


def wait_for(path: Path, seconds: float = 30) -> None:
    end = time.monotonic() + seconds
    while not path.exists():
        assert time.monotonic() < end, f"{path} never appeared"
        time.sleep(0.05)


def test_a_signal_makes_the_cli_exit_130_with_run_results(tmp_path: Path) -> None:
    if not has_console():
        pytest.skip("CTRL_BREAK_EVENT needs a console")
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": HEAD + ENGINE,
            "models/m.yml": CHAIN.replace('"m:body"', '"m:body"'),
            "src/m.py": SLOW,
        },
    )
    kwargs: dict[str, Any] = {}
    sig: int = signal.SIGINT
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        sig = signal.CTRL_BREAK_EVENT
    argv = [sys.executable, "-m", "pipeloom", "run", "--project-dir", str(root), "--target", "ci"]
    argv += ["--run-id", "r1", "--cancel-grace", "10s", "--log-format", "json"]
    proc = subprocess.Popen(
        argv,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env={
            **os.environ,
            "PIPELOOM_USER": "t",
            "PIPELOOM_TARGET": "",
            "PL_MARK": str(root / "started"),
        },
        **kwargs,
    )
    try:
        wait_for(root / "started")
        proc.send_signal(sig)
        code = proc.wait(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    assert code == 130
    assert statuses(root, "r1")["b"] == "cancelled"


def test_a_parent_cancel_reaches_the_child_and_leaves_no_orphan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    if not has_console():
        pytest.skip("CTRL_BREAK_EVENT needs a console")
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": TASK_YML,
            "models/m.yml": CHAIN,
            "src/m.py": SLOW,
        },
    )
    made: list[ProcessExecutor] = []
    original = ProcessExecutor.__init__

    def spy(self: ProcessExecutor, *args: Any, **kwargs: Any) -> None:
        original(self, *args, **kwargs)
        made.append(self)

    monkeypatch.setattr(ProcessExecutor, "__init__", spy)
    control = RunControl()
    threading.Thread(
        target=lambda: (wait_for(root / "started"), control.request_stop()), daemon=True
    ).start()
    project = Project(
        ProjectOptions(
            project_dir=root,
            target="ci",
            env={"PIPELOOM_USER": "t", "PL_MARK": str(root / "started")},
        )
    )
    sink = RecordingSink()
    with EventBus([(sink, 5)], clock=SystemClock(), invocation_id="inv") as bus:
        outcome = project.run(RunOptions(run_id="run1", workers=1, cancel_grace=10), bus, control)
    assert (outcome.status, outcome.exit_code) == ("cancelled", 130), outcome.diagnostics
    popens = [p for e in made for p in e.popens]
    assert len(popens) == 1
    assert all(p.poll() is not None for p in popens)  # no orphan
    assert popens[0].returncode == 130  # the child stopped itself
    assert statuses(root)["b"] == "cancelled"
    assert sink.types().count("run.cancel_requested") == 1
