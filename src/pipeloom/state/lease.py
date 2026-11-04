"""Lock leases: owner names and the heartbeat that keeps a held lock alive."""

from __future__ import annotations

import os
import socket
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import timedelta

from pipeloom.platform import Clock, SystemClock
from pipeloom.state.types import HEARTBEAT_SECONDS, LEASE_SECONDS, LockInfo


def new_owner() -> str:
    """A fresh owner name, `<host>:<pid>:<uuid8>`, one per lock acquisition."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def new_lock_info(
    scope: str, run_id: str, clock: Clock, lease_seconds: float = LEASE_SECONDS
) -> LockInfo:
    now = clock.now()
    return LockInfo(scope, run_id, new_owner(), now, now + timedelta(seconds=lease_seconds))


def expired(info: LockInfo, clock: Clock) -> bool:
    return info.expires_at <= clock.now()


class Heartbeat:
    """The last renewal failure of a held lock, for the run to report."""

    def __init__(self) -> None:
        self.last_error: Exception | None = None


@contextmanager
def held(
    info: LockInfo,
    renew: Callable[[LockInfo], None],
    release: Callable[[LockInfo], None],
    interval: float = HEARTBEAT_SECONDS,
    status: Heartbeat | None = None,
) -> Iterator[LockInfo]:
    """Hold an acquired lock: renew it from a daemon thread, release it on exit.

    A failed renewal is recorded in `status.last_error` and retried at the next beat; only the
    release stops the thread.
    """
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(interval):
            try:
                renew(info)
            except Exception as err:  # noqa: BLE001 - a transient failure must not stop renewal
                if status is not None:
                    status.last_error = err

    thread = threading.Thread(target=beat, name=f"pipeloom-lease-{info.scope}", daemon=True)
    thread.start()
    try:
        yield info
    finally:
        stop.set()
        thread.join(timeout=5)
        release(info)


__all__ = ["Heartbeat", "SystemClock", "expired", "held", "new_lock_info", "new_owner"]
