"""The trigger a watcher defers on (ADR-0048, rule 7). It reads only the unit's files: never task
states, which a trigger must not ask for (a triggerer deadlock fixed in Airflow 3.2.2). It lives in
the installed package, so the triggerer can import it, and serializes only paths and numbers."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any, Literal

from pipeloom.runtime.retry import UnitPolicy
from pipeloom_airflow._compat import BaseTrigger, TriggerEvent
from pipeloom_airflow.watcher.state import VISIBILITY_GRACE_S, decide, read_unit

EVENT_VERSION = 1
# Airflow leaves these untyped; called through Any so strict mypy accepts them on every platform.
_event: Any = TriggerEvent
_base_init: Any = BaseTrigger.__init__


class PipeloomWatcherTrigger(BaseTrigger):
    """Reads unit `unit_id` under `run_dir` every `poll_interval` seconds. Yields `done` (a final
    attempt: `outcome` success or failed), `run` (the results row says the watcher runs the unit),
    `invisible` (the results name a verdict whose file stays unseen for the grace) or, after
    `recheck_after` seconds without any of these, `pending`."""

    sleep: Callable[[float], Awaitable[Any]] = staticmethod(asyncio.sleep)
    monotonic: Callable[[], float] = staticmethod(time.monotonic)
    """The clock; tests replace both."""

    def __init__(
        self,
        *,
        run_dir: str,
        unit_id: str,
        retries: int,
        retry_on: Literal["transient", "any"],
        poll_interval: float,
        recheck_after: float,
    ) -> None:
        _base_init(self)
        self.run_dir = run_dir
        self.unit_id = unit_id
        self.retries = retries
        self.retry_on = retry_on
        self.poll_interval = poll_interval
        self.recheck_after = recheck_after

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "run_dir": self.run_dir,
                "unit_id": self.unit_id,
                "retries": self.retries,
                "retry_on": self.retry_on,
                "poll_interval": self.poll_interval,
                "recheck_after": self.recheck_after,
            },
        )

    def _payload(self, status: str, **extra: Any) -> TriggerEvent:
        event: TriggerEvent = _event(
            {"v": EVENT_VERSION, "status": status, "unit_id": self.unit_id, **extra}
        )
        return event

    async def run(self) -> AsyncIterator[TriggerEvent]:
        clock = type(self).monotonic
        policy = UnitPolicy(self.retries, 0.0, self.retry_on, None)
        start = clock()
        unseen_since: float | None = None
        while True:
            view = await asyncio.to_thread(
                read_unit, Path(self.run_dir), self.unit_id, probe=self.retries + 1
            )
            d = decide(view, policy, producer_ended=False)
            now = clock()
            if d.kind in ("success", "failed"):
                number = d.attempt.number if d.attempt is not None else None
                yield self._payload(
                    "done", outcome=d.kind, attempt=number, code=d.code, message=d.message
                )
                return
            if d.kind == "run":
                yield self._payload("run")
                return
            if d.kind == "invisible":
                unseen_since = now if unseen_since is None else unseen_since
                if now - unseen_since >= VISIBILITY_GRACE_S:
                    yield self._payload("invisible", code=d.code, message=d.message)
                    return
            else:
                unseen_since = None
            if now - start >= self.recheck_after:
                yield self._payload("pending")
                return
            await type(self).sleep(self.poll_interval)
