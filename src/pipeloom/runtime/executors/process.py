"""Run each unit in its own `python -m pipeloom run` process (`boundary: task`).

Only argv crosses the boundary: the manifest path, the unit id, the run id and the plan hash.
The child writes its events as jsonl on stderr; a reader thread relays them to the parent's bus
with `EventBus.forward`, so they keep the child's invocation id and sequence numbers. The child's
attempt file is the result. No attempt file means the child died before it could write one.
"""

from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from pipeloom.artifacts.io import write_json_atomic
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import result_row
from pipeloom.events.bus import EventBus
from pipeloom.events.model import ErrorInfo, Event, UnitFailed
from pipeloom.runtime.executors import UnitCompletion, UnitHandle
from pipeloom.runtime.runner import ModelResult

PARENT_ENV = "PIPELOOM_PARENT_INVOCATION_ID"
"""Set in the child: it opens no project sinks and reports to the parent only."""
LOG_TAIL = 20
POLL_SECONDS = 0.2
_NOT_RELAYED = ("invocation.",)


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
        if self.vars:
            out += ["--vars", self.vars]
        for spec in self.plugins:
            out += ["--plugin", spec]
        return out


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
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self._cmd = command
        self._paths = paths
        self._bus = bus
        self._env = {**env, "PYTHONIOENCODING": "utf-8", PARENT_ENV: bus.invocation_id}
        self._unit_nodes = unit_nodes
        """Unit id to the unique id of its persisted model, for the row of a crashed unit."""
        self._done: queue.SimpleQueue[UnitCompletion] = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._procs: dict[str, subprocess.Popen[str]] = {}
        self._threads: list[threading.Thread] = []

    # -- Executor ---------------------------------------------------------------------------

    def submit(self, unit_id: str, *, queued_at: float) -> UnitHandle:
        thread = threading.Thread(
            target=self._work, args=(unit_id,), name=f"pipeloom-child-{unit_id}", daemon=True
        )
        with self._lock:
            self._threads.append(thread)
        thread.start()
        return UnitHandle(unit_id)

    def wait_any(self) -> UnitCompletion:
        while True:  # a timeout keeps Ctrl+C deliverable on Windows
            try:
                return self._done.get(timeout=POLL_SECONDS)
            except queue.Empty:
                continue

    def cancel(self, handle: UnitHandle) -> bool:
        return False  # a child cannot be stopped before 0.3

    def shutdown(self, *, wait: bool) -> None:
        with self._lock:
            threads, procs = list(self._threads), list(self._procs.values())
        if not wait:
            for proc in procs:
                if proc.poll() is None:
                    proc.kill()
        for thread in threads:
            thread.join()

    # -- one child --------------------------------------------------------------------------

    def _work(self, unit_id: str) -> None:
        try:
            completion = self._run(unit_id)
        except BaseException as exc:  # noqa: BLE001 - a lost completion would hang wait_any
            completion = UnitCompletion(unit_id, "failed", internal_error=exc)
        self._done.put(completion)

    def _run(self, unit_id: str) -> UnitCompletion:
        paths = self._paths
        floor = paths.next_attempt(unit_id)
        log_path = paths.log(unit_id, floor)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        tail: deque[str] = deque(maxlen=LOG_TAIL)
        write_lock = threading.Lock()
        with log_path.open("w", encoding="utf-8", newline="\n") as log:

            def note(line: str) -> None:
                with write_lock:
                    log.write(line + "\n")
                    log.flush()
                    tail.append(line)

            proc = subprocess.Popen(
                self._cmd.argv(unit_id),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=self._env,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            with self._lock:
                self._procs[unit_id] = proc
            assert proc.stdout is not None and proc.stderr is not None
            out_reader = threading.Thread(target=_pump, args=(proc.stdout, note), daemon=True)
            out_reader.start()
            for line in proc.stderr:
                self._relay(line.rstrip("\r\n"), note)
            code = proc.wait()
            out_reader.join()
        return self._completion(unit_id, floor, code, list(tail), log_path)

    def _relay(self, line: str, note: Any) -> None:
        if line.startswith("{"):
            try:
                event = Event.model_validate(json.loads(line))
            except ValueError:  # not an event: keep it in the log
                note(line)
                return
            if not event.type.startswith(_NOT_RELAYED) and event.type != "diagnostic":
                self._bus.forward(event)
            return
        note(line)

    def _completion(
        self, unit_id: str, floor: int, code: int, tail: list[str], log_path: Path
    ) -> UnitCompletion:
        paths = self._paths
        numbers = [n for n in paths.attempt_numbers(unit_id) if n >= floor]
        if numbers:
            raw = paths.attempt(unit_id, numbers[-1]).read_bytes().decode("utf-8")
            row: dict[str, Any] = json.loads(raw)
            error = ErrorInfo.model_validate(row["error"]) if row.get("error") else None
            status: Literal["succeeded", "failed"] = (
                "succeeded" if row["status"] == "succeeded" else "failed"
            )
            return UnitCompletion(
                unit_id, status, ModelResult(unit_id, status, numbers[-1], row, error)
            )
        error = ErrorInfo(
            code="PLX106",
            category="transient",
            message=f"the process of unit '{unit_id}' exited with code {code} and wrote no result",
            hint=f"read its log, {log_path.relative_to(paths.root).as_posix()}, under the artifacts folder",
            trace_excerpt=tail,
        )
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        row = result_row(
            unique_id=self._unit_nodes[unit_id],
            unit_id=unit_id,
            status="failed",
            attempt=floor,
            started_at=now,
            ended_at=now,
            executor=self.name,
            error=error.model_dump(mode="json"),
        )
        write_json_atomic(paths.attempt(unit_id, floor), row)
        self._bus.emit(
            UnitFailed(error=error, duration_ms=0), unit_id=unit_id, model=unit_id, attempt=floor
        )
        return UnitCompletion(unit_id, "failed", ModelResult(unit_id, "failed", floor, row, error))


def _pump(stream: Any, note: Any) -> None:
    for line in stream:
        note(line.rstrip("\r\n"))
