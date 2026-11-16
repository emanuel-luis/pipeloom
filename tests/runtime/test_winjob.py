"""The Windows job object helper of the process executor."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import pytest

from pipeloom.runtime.executors import _winjob

windows = pytest.mark.skipif(sys.platform != "win32", reason="job objects exist only on Windows")


def _child(beat: Path) -> subprocess.Popen[str]:
    """A child that starts a beat-writing grandchild once it reads a line, then sleeps."""
    grandchild = (
        "import time, pathlib; p = pathlib.Path(" + repr(str(beat)) + "); "
        "exec('while True: p.write_text(str(time.time())); time.sleep(0.1)')"
    )
    code = (
        "import subprocess, sys, time; sys.stdin.readline(); "
        f"subprocess.Popen([sys.executable, '-c', {grandchild!r}]); time.sleep(60)"
    )
    return subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE, text=True)


def _start_tree(beat: Path) -> tuple[subprocess.Popen[str], _winjob.Job]:
    proc = _child(beat)
    job = _winjob.create(proc.pid)
    assert job is not None
    assert proc.stdin is not None
    proc.stdin.write("go\n")  # the grandchild starts only once the child is in the job
    proc.stdin.close()
    deadline = time.monotonic() + 20
    while not beat.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert beat.exists(), "the grandchild never started"
    return proc, job


def _assert_no_beat(beat: Path) -> None:
    time.sleep(0.5)  # a live grandchild would have written by now
    before = beat.read_text(encoding="utf-8")
    time.sleep(0.5)
    assert beat.read_text(encoding="utf-8") == before


@windows
def test_terminate_ends_the_child_and_its_grandchild(tmp_path: Path) -> None:
    beat = tmp_path / "beat.txt"
    proc, job = _start_tree(beat)
    try:
        job.terminate()
        assert proc.wait(timeout=10) is not None
        _assert_no_beat(beat)
    finally:
        job.close()
        proc.kill()


@windows
def test_close_ends_the_tree_and_terminate_after_close_does_nothing(tmp_path: Path) -> None:
    beat = tmp_path / "beat.txt"
    proc, job = _start_tree(beat)
    try:
        job.close()  # KILL_ON_JOB_CLOSE: what happens when the parent dies
        assert proc.wait(timeout=10) is not None
        _assert_no_beat(beat)
        job.terminate()
        job.close()
    finally:
        proc.kill()


@pytest.mark.skipif(sys.platform == "win32", reason="checks the fallback off Windows")
def test_create_returns_none_off_windows() -> None:
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    try:
        assert _winjob.create(proc.pid) is None
    finally:
        proc.wait()
