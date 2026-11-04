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


@contextmanager
def held(
    info: LockInfo,
    renew: Callable[[LockInfo], None],
    release: Callable[[LockInfo], None],
    interval: float = HEARTBEAT_SECONDS,
) -> Iterator[LockInfo]:
    """Hold an acquired lock: renew it from a daemon thread, release it on exit."""
    stop = threading.Event()

    def beat() -> None:
        while not stop.wait(interval):
            try:
                renew(info)
            except Exception:  # noqa: BLE001 - the thread must not die loudly; the lease lapses
                return

    thread = threading.Thread(target=beat, name=f"pipeloom-lease-{info.scope}", daemon=True)
    thread.start()
    try:
        yield info
    finally:
        stop.set()
        thread.join(timeout=5)
        release(info)


__all__ = ["SystemClock", "expired", "held", "new_lock_info", "new_owner"]
