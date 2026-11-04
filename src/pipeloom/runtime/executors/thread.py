"""Run units on a thread pool in the current process (`boundary: process`)."""

from __future__ import annotations

import queue
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Literal

from pipeloom.runtime.executors import UnitCompletion, UnitHandle


class ThreadExecutor:
    name: str = "thread"
    boundary: Literal["process", "task"] = "process"

    def __init__(self, workers: int, run_unit: Callable[[str, float], UnitCompletion]) -> None:
        self._run_unit = run_unit
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="pipeloom-unit")
        self._done: queue.SimpleQueue[UnitCompletion] = queue.SimpleQueue()

    def _work(self, unit_id: str, queued_at: float) -> None:
        try:
            completion = self._run_unit(unit_id, queued_at)
        except BaseException as exc:  # noqa: BLE001 - a lost completion would hang wait_any
            completion = UnitCompletion(unit_id, "failed", internal_error=exc)
        self._done.put(completion)

    def submit(self, unit_id: str, *, queued_at: float) -> UnitHandle:
        self._pool.submit(self._work, unit_id, queued_at)
        return UnitHandle(unit_id)

    def wait_any(self) -> UnitCompletion:
        return self._done.get()

    def cancel(self, handle: UnitHandle) -> bool:
        return False  # threads cannot be stopped; cooperative cancellation arrives in 0.3

    def shutdown(self, *, wait: bool) -> None:
        self._pool.shutdown(wait=wait, cancel_futures=not wait)
