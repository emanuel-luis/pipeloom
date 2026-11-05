"""The trigger that waits on a Databricks job run in the triggerer.

It lives in the installed package, never in a DAG bundle, so the triggerer can import it. It
serializes only ids and numbers: the connection is looked up again by its id, so no secret is
stored with the deferred task. `run` may run twice for one deferral; its events are the same.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from pipeloom.diagnostics import Report
from pipeloom_airflow._compat import BaseTrigger, TriggerEvent, aget_connection, mask_secret
from pipeloom_airflow.databricks.connection import credentials_from_connection, make_client
from pipeloom_airflow.databricks.mirror import run_ended, snapshot, task_verdict
from pipeloom_databricks.api import TERMINAL, ApiError, ConnectionNotConfigured, DatabricksClient

EVENT_VERSION = 1
# Airflow leaves these untyped; called through Any so strict mypy accepts them on every platform.
_event: Any = TriggerEvent
_base_init: Any = BaseTrigger.__init__


def _error(message: str) -> TriggerEvent:
    event: TriggerEvent = _event({"v": EVENT_VERSION, "status": "error", "message": message})
    return event


class DatabricksRunTrigger(BaseTrigger):
    """Polls one job run every `poll_interval` seconds.

    `task_key=None` (the `shared` poll mode) waits until the run, and the repair `repair_id` when
    set, is over, then yields the run's snapshot. With a task key it yields that task's verdict as
    soon as there is one. An API or connection failure yields an error event.
    """

    sleep: Callable[[float], Awaitable[Any]] = staticmethod(asyncio.sleep)
    """The clock; tests replace it."""

    def __init__(
        self,
        *,
        conn_id: str,
        job_run_id: int,
        task_key: str | None,
        baseline_attempt: int,
        max_attempts: int,
        poll_interval: float,
        client_factory: str | None,
        repair_id: int | None = None,
    ) -> None:
        _base_init(self)
        self.conn_id = conn_id
        self.job_run_id = job_run_id
        self.task_key = task_key
        self.baseline_attempt = baseline_attempt
        self.max_attempts = max_attempts
        self.poll_interval = poll_interval
        self.client_factory = client_factory
        self.repair_id = repair_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {
                "conn_id": self.conn_id,
                "job_run_id": self.job_run_id,
                "task_key": self.task_key,
                "baseline_attempt": self.baseline_attempt,
                "max_attempts": self.max_attempts,
                "poll_interval": self.poll_interval,
                "client_factory": self.client_factory,
                "repair_id": self.repair_id,
            },
        )

    async def _client(self) -> DatabricksClient | str:
        """The client, or the reason there is none."""
        creds = None
        if not self.client_factory:
            conn = await aget_connection(self.conn_id)
            report = Report()
            creds = credentials_from_connection(conn, self.conn_id, report)
            if creds is None:
                return "; ".join(f"{d.code} {d.message}" for d in report.sorted())
            for secret in creds.secrets():
                mask_secret(secret)
        try:
            return await asyncio.to_thread(make_client, creds, factory=self.client_factory)
        except ConnectionNotConfigured as err:
            return f"PLD010 {err}"

    async def run(self) -> AsyncIterator[TriggerEvent]:
        client = await self._client()
        if isinstance(client, str):
            yield _error(client)
            return
        seen_active = False
        while True:
            try:
                run = await asyncio.to_thread(client.get_run, self.job_run_id)
            except ApiError as err:
                yield _error(f"PLD009 Databricks API request failed (HTTP {err.status}): {err}")
                return
            if self.task_key is None:
                if run_ended(run, self.repair_id, seen_active):
                    yield _event(
                        {
                            "v": EVENT_VERSION,
                            "status": "done",
                            "job_run_id": self.job_run_id,
                            "snapshot": snapshot(run),
                        }
                    )
                    return
                seen_active = seen_active or run.life_cycle_state not in TERMINAL
            else:
                verdict = task_verdict(
                    run,
                    self.task_key,
                    baseline_attempt=self.baseline_attempt,
                    max_attempts=self.max_attempts,
                )
                if verdict is not None:
                    yield _event(
                        {
                            "v": EVENT_VERSION,
                            "status": "done",
                            "job_run_id": self.job_run_id,
                            "task_key": self.task_key,
                            "attempt": verdict.attempt,
                            "outcome": verdict.outcome,
                            "message": verdict.message,
                            "run_page_url": run.run_page_url,
                        }
                    )
                    return
            await type(self).sleep(self.poll_interval)
