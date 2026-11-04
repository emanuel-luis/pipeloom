"""Run each unit in its own `python -m pipeloom run` process (`boundary: task`).

Only argv crosses the boundary: the manifest path, the unit id, the run id and the plan hash.
The child writes its events as jsonl on stderr; a reader thread relays them to the parent's bus
with `EventBus.forward`, so they keep the child's invocation id and sequence numbers. The child's
attempt file is the result. No attempt file means the child died before it could write one.

Every child starts in its own process group, so a terminal Ctrl+C reaches only the owner. The
child enforces a unit's timeout itself; the parent's own deadline is `timeout + cancel_grace`.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pipeloom.artifacts.io import write_json_once
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import result_row
from pipeloom.events.bus import EventBus
from pipeloom.events.model import ErrorInfo, Event, UnitFailed
from pipeloom.runtime.control import Reason, interrupt_error
from pipeloom.runtime.executors import UnitCompletion, UnitHandle
from pipeloom.runtime.runner import ModelResult

PARENT_ENV = "PIPELOOM_PARENT_INVOCATION_ID"
"""Set in the child: it opens no project sinks and reports to the parent only."""
LOG_TAIL = 20
KILL_TIMEOUT = 10.0
JOIN_TIMEOUT = 5.0
"""Seconds to wait for relay threads and children once a kill was sent."""
_NOT_RELAYED = ("invocation.",)
_TERMINAL = frozenset({"unit.succeeded", "unit.failed", "unit.timed_out", "unit.cancelled"})


@dataclass(frozen=True)
class ChildCommand:
    project_dir: Path
    target: str
    manifest: Path
    run_id: str
    plan_hash: str
    full_refresh: bool
    vars: str | None
    """`--vars` as given: a JSON mapping, or None."""
    plugins: tuple[str, ...]
    cancel_grace: float | None = None
    """The parent's grace in seconds, rounded up: the flag takes whole seconds."""

    def argv(self, unit_id: str) -> list[str]:
        out = [
            sys.executable,
            "-m",
            "pipeloom",
            "run",
            "--log-format",
            "json",
            "--project-dir",
            str(self.project_dir),
            "--target",
            self.target,
            "--manifest",
            str(self.manifest),
            "--select",
            f"unit:{unit_id}",
            "--run-id",
            self.run_id,
            "--expect-plan-hash",
            self.plan_hash,
            "--full-refresh",
            "true" if self.full_refresh else "false",
        ]
        if self.cancel_grace is not None:
            out += ["--cancel-grace", str(math.ceil(self.cancel_grace))]
        if self.vars:
            out += ["--vars", self.vars]
        for spec in self.plugins:
            out += ["--plugin", spec]
        return out


def _kill(proc: subprocess.Popen[str]) -> None:
    """Kill the child and everything it started: a grandchild would keep the pipes open."""
    if proc.poll() is not None:
        return  # reaped: its pid, and so its group id, may belong to someone else now
    if sys.platform == "win32":
        with contextlib.suppress(OSError, subprocess.SubprocessError):
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                capture_output=True,
                timeout=KILL_TIMEOUT,
                check=False,
            )
    else:  # the child leads its own session, so its group id is its pid
        with contextlib.suppress(OSError):
            os.killpg(proc.pid, signal.SIGKILL)
    if proc.poll() is None:
        with contextlib.suppress(OSError):
            proc.kill()


def _pump(stream: Any, note: Any) -> None:
    for line in stream:
        note(line.rstrip("\r\n"))


class ProcessExecutor:
    name: str = "process"
    boundary: Literal["process", "task"] = "task"

    def __init__(
        self,
        *,
        workers: int,
        command: ChildCommand,
        paths: RunPaths,
        bus: EventBus,
        env: Mapping[str, str],
        unit_nodes: Mapping[str, str],
        cancel_grace: float = 30.0,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self._cmd = command
        self._paths = paths
        self._bus = bus
        self._env = {**env, "PYTHONIOENCODING": "utf-8", PARENT_ENV: bus.invocation_id}
        self._unit_nodes = unit_nodes
        """Unit id to the unique id of its persisted model, for the row of a crashed unit."""
        self._grace = cancel_grace
        self._done: queue.SimpleQueue[UnitCompletion | None] = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._seq = 0
        self._procs: dict[UnitHandle, subprocess.Popen[str]] = {}
        self._threads: list[threading.Thread] = []
        self._stopped: dict[UnitHandle, Reason] = {}
        self._forwarded: set[tuple[str, int | None]] = set()
        self.popens: list[subprocess.Popen[str]] = []
        """Every child ever started, so a test can check that none outlived the run."""

    # -- Executor ---------------------------------------------------------------------------

    def submit(self, unit_id: str, *, queued_at: float) -> UnitHandle:
        with self._lock:
            self._seq += 1
            handle = UnitHandle(unit_id, self._seq, self._paths.next_attempt(unit_id))
            thread = threading.Thread(
                target=self._work, args=(handle,), name=f"pipeloom-child-{unit_id}", daemon=True
            )
            self._threads.append(thread)
        thread.start()
        return handle

    def wait_any(self, timeout: float | None) -> UnitCompletion | None:
        try:
            return self._done.get(timeout=timeout)
        except queue.Empty:
            return None

    def wake(self) -> None:
        self._done.put(None)

    def has_terminal_event(self, handle: UnitHandle) -> bool:
        with self._lock:
            return (handle.unit_id, handle.attempt) in self._forwarded

    def cancel(self, handle: UnitHandle, reason: Reason, *, final: bool = False) -> bool:
        """Ask the child to stop (SIGTERM, CTRL_BREAK on Windows) and kill it after the grace.

        A timeout is already past its grace in the scheduler, so the child is killed at once.
        """
        with self._lock:
            proc = self._procs.get(handle)
            self._stopped.setdefault(handle, reason)
        if proc is None or proc.poll() is not None:
            return True
        if reason == "timeout" or final:
            _kill(proc)
            return True
        try:
            if sys.platform == "win32":
                proc.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                proc.send_signal(signal.SIGTERM)
        except OSError:
            _kill(proc)
            return True
        timer = threading.Timer(self._grace, _kill, args=(proc,))
        timer.daemon = True
        timer.start()
        return True

    def shutdown(self, *, wait: bool) -> None:
        with self._lock:
            threads, procs = list(self._threads), list(self._procs.values())
        if not wait:
            for proc in procs:
                _kill(proc)
        end = time.monotonic() + (JOIN_TIMEOUT if not wait else 4 * JOIN_TIMEOUT)
        for thread in threads:
            thread.join(max(0.0, end - time.monotonic()))
        if wait:  # whatever outlived the join is stuck: end it rather than hang the run
            for proc in procs:
                _kill(proc)

    # -- one child --------------------------------------------------------------------------

    def _work(self, handle: UnitHandle) -> None:
        try:
            completion = self._run(handle)
        except BaseException as exc:  # noqa: BLE001 - a lost completion would hang wait_any
            completion = UnitCompletion(handle, "failed", handle.attempt, internal_error=exc)
        self._done.put(completion)

    def _run(self, handle: UnitHandle) -> UnitCompletion:
        paths, unit_id = self._paths, handle.unit_id
        log_path = paths.log(unit_id, handle.attempt)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        tail: deque[str] = deque(maxlen=LOG_TAIL)
        write_lock = threading.Lock()
        # an if statement, not a conditional expression: mypy narrows sys.platform only in
        # statements, and CREATE_NEW_PROCESS_GROUP exists only in the Windows stubs
        group: dict[str, Any]
        if sys.platform == "win32":
            group = {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
        else:
            group = {"start_new_session": True}
        with log_path.open("w", encoding="utf-8", newline="\n") as log:

            def note(line: str) -> None:
                with write_lock:
                    log.write(line + "\n")
                    log.flush()
                    tail.append(line)

            with self._lock:
                proc = subprocess.Popen(
                    self._cmd.argv(unit_id),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=self._env,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    **group,
                )
                self._procs[handle] = proc
                self.popens.append(proc)
            assert proc.stdout is not None and proc.stderr is not None
            out_reader = threading.Thread(target=_pump, args=(proc.stdout, note), daemon=True)
            out_reader.start()
            err_reader = threading.Thread(
                target=self._pump_events, args=(proc.stderr, note), daemon=True
            )
            err_reader.start()
            code = proc.wait()
            if sys.platform != "win32":  # sweep what the child left behind in its group
                with contextlib.suppress(OSError):
                    os.killpg(proc.pid, signal.SIGKILL)
            # A grandchild may keep a pipe open after the child is gone: do not wait for it.
            err_reader.join(JOIN_TIMEOUT)
            out_reader.join(JOIN_TIMEOUT)
        return self._completion(handle, code, list(tail), log_path)

    def _pump_events(self, stream: Any, note: Any) -> None:
        for line in stream:
            self._relay(line.rstrip("\r\n"), note)

    def _relay(self, line: str, note: Any) -> None:
        if line.startswith("{"):
            try:
                event = Event.model_validate(json.loads(line))
            except ValueError:  # not an event: keep it in the log
                note(line)
                return
            if event.type == "diagnostic":  # the reason a child stopped before its unit ran
                where = event.data.get("location")
                at = ""
                if isinstance(where, dict) and where.get("file"):
                    at = f" ({where['file']}" + (f":{where['line']})" if where.get("line") else ")")
                note(f"{event.data.get('code')} {event.data.get('message')}{at}")
            elif not event.type.startswith(_NOT_RELAYED):
                if event.type in _TERMINAL:
                    with self._lock:
                        self._forwarded.add((event.unit_id or "", event.attempt))
                self._bus.forward(event)
            return
        note(line)

    def _completion(
        self, handle: UnitHandle, code: int, tail: list[str], log_path: Path
    ) -> UnitCompletion:
        paths, unit_id, floor = self._paths, handle.unit_id, handle.attempt
        numbers = [n for n in paths.attempt_numbers(unit_id) if n >= floor]
        if numbers:
            raw = paths.attempt(unit_id, numbers[-1]).read_bytes().decode("utf-8")
            row: dict[str, Any] = json.loads(raw)
            error = ErrorInfo.model_validate(row["error"]) if row.get("error") else None
            status: Literal["succeeded", "failed", "timed_out", "cancelled"] = row["status"]
            return UnitCompletion(
                handle,
                status,
                numbers[-1],
                None if error is None else error.category,
                ModelResult(unit_id, status, numbers[-1], row, error),
            )
        with self._lock:
            stopped = self._stopped.get(handle)
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if stopped is not None:  # killed by the scheduler: the verdict is the scheduler's
            error = interrupt_error(stopped, unit_id, floor, None)
            status = "timed_out" if stopped == "timeout" else "cancelled"
        else:
            error = ErrorInfo(
                code="PLX106",
                category="transient",
                message=f"the process of unit '{unit_id}' exited with code {code} and wrote no result",
                hint=f"read its log, {log_path.relative_to(paths.root).as_posix()}, under the artifacts folder",
                trace_excerpt=tail,
            )
            status = "failed"
        row = result_row(
            unique_id=self._unit_nodes[unit_id],
            unit_id=unit_id,
            status=status,
            attempt=floor,
            started_at=now,
            ended_at=now,
            executor=self.name,
            error=error.model_dump(mode="json"),
        )
        write_json_once(paths.attempt(unit_id, floor), row)
        if stopped is None:
            self._bus.emit(
                UnitFailed(error=error, duration_ms=0),
                unit_id=unit_id,
                model=unit_id,
                attempt=floor,
            )
        return UnitCompletion(
            handle, status, floor, error.category, ModelResult(unit_id, status, floor, row, error)
        )
