"""Turn SIGINT and SIGTERM into a graceful stop of the run (ADR-0029)."""

from __future__ import annotations

import signal
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType

from pipeloom.runtime.control import RunControl


def stop_signals() -> tuple[signal.Signals, ...]:
    """Ctrl+C and the polite kill: SIGTERM on POSIX, Ctrl+Break (SIGBREAK) on Windows."""
    if sys.platform == "win32":
        return (signal.SIGINT, signal.SIGBREAK)
    return (signal.SIGINT, signal.SIGTERM)


@contextmanager
def install_stop_handlers(control: RunControl) -> Iterator[None]:
    """The first signal calls `control.request_stop()`, the second `request_stop(hard=True)`, the
    third raises KeyboardInterrupt. Handlers only set flags; `RunControl` wakes the scheduler.

    Only the main thread can install handlers; elsewhere this does nothing. The previous
    handlers come back on exit.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return
    seen = 0

    def handle(signum: int, _frame: FrameType | None) -> None:
        nonlocal seen
        seen += 1
        if seen >= 3:
            raise KeyboardInterrupt
        control.request_stop(hard=seen == 2, signal_name=signal.Signals(signum).name)

    previous: dict[signal.Signals, object] = {}
    try:
        for sig in stop_signals():
            previous[sig] = signal.signal(sig, handle)
        yield
    finally:
        for sig, old in previous.items():
            signal.signal(sig, old)  # type: ignore[arg-type]
