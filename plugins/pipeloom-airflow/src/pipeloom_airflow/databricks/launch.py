"""The launch task of DATABRICKS mode: start, continue or repair the job run. In `per_task` poll
mode it returns at once; in `shared` mode it waits for the run in the triggerer and keeps its
snapshot in the XCom the sensors read."""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

from pydantic import JsonValue

from pipeloom.diagnostics import Report, Severity, format_diagnostic
from pipeloom.graph.selectors import Selection
from pipeloom_airflow._compat import (
    AirflowException,
    AirflowFailException,
    BaseOperator,
    Context,
    get_connection,
    mask_secret,
)
from pipeloom_airflow.commands import CoreCommand
from pipeloom_airflow.databricks.connection import credentials_from_connection, make_client
from pipeloom_airflow.databricks.launch_logic import load_project, start_or_attach
from pipeloom_airflow.databricks.triggers import DatabricksRunTrigger
from pipeloom_airflow.operators.local import _context
from pipeloom_databricks.api import ConnectionNotConfigured, DatabricksClient

RETRYABLE = frozenset({"PLD009"})
"""Codes a retry of the launch may clear (the API was unreachable)."""


def _raise(report: Report) -> None:
    errors = [d for d in report.sorted() if d.severity is Severity.ERROR]
    text = "\n".join(format_diagnostic(d) for d in errors) or "the launch failed"
    if {d.code for d in errors} <= RETRYABLE:
        raise AirflowException(text)
    raise AirflowFailException(text)


def client_for(conn_id: str, factory: str | None, report: Report) -> DatabricksClient | None:
    """A client for Airflow connection `conn_id` (secrets masked), or None after PLD010."""
    creds = None
    if not factory:
        creds = credentials_from_connection(get_connection(conn_id), conn_id, report)
        if creds is None:
            return None
        for secret in creds.secrets():
            mask_secret(secret)
    try:
        return make_client(creds, factory=factory)
    except ConnectionNotConfigured as err:
        report.add("PLD010", str(err), context={"conn_id": conn_id})
        return None


class PipeloomDatabricksRunOperator(BaseOperator):
    """Starts or continues the job run of one DAG run. A cleared launch attaches to the same run
    (or repairs it); Databricks owns the retries of its tasks."""

    def __init__(
        self,
        *,
        command: CoreCommand,
        pipeline: str,
        conn_id: str,
        ensure_job: bool,
        select: tuple[str, ...],
        exclude: tuple[str, ...],
        poll_interval: float,
        client_factory: str | None,
        group_id: str | None,
        poll: Literal["per_task", "shared"] = "shared",
        wait_timeout_s: float = 86400.0,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("retries", 2)
        super().__init__(**kwargs)
        self.poll = poll
        self.command = command
        self.pipeline = pipeline
        self.conn_id = conn_id
        self.ensure_job = ensure_job
        self.select = select
        self.exclude = exclude
        self.poll_interval = poll_interval
        self.client_factory = client_factory
        self.group_id = group_id
        self.wait_timeout_s = wait_timeout_s

    def _client(self, report: Report) -> DatabricksClient | None:
        return client_for(self.conn_id, self.client_factory, report)

    def execute(self, context: Context) -> dict[str, JsonValue] | None:
        ctx = _context(context, self.group_id, self.task_id)
        report = Report()
        cmd = self.command
        loaded = load_project(
            cmd.project_dir,
            cmd.target,
            json.loads(cmd.vars_json) if cmd.vars_json else {},
            expect_plan_hash=cmd.plan_hash,
            env=os.environ,
            report=report,
        )
        client = self._client(report) if loaded is not None else None
        result = None
        if loaded is not None and client is not None:
            plan, runtime = loaded
            result = start_or_attach(
                client,
                Path(cmd.project_dir),
                plan,
                self.pipeline,
                runtime,
                run_id=ctx.run_id,
                full_refresh=ctx.full_refresh,
                selection=Selection(self.select, self.exclude),
                ensure_job=self.ensure_job,
                report=report,
            )
        for d in report.sorted():
            if d.severity is not Severity.ERROR:
                self.log.warning("%s", format_diagnostic(d))
        if result is None:
            _raise(report)
            return None
        self.log.info(
            "pipeloom run %s: %s job run %s (%s)",
            result.run_id,
            result.action,
            result.job_run_id,
            result.run_page_url or "no page url",
        )
        if self.poll == "per_task":
            return result.document()
        self.defer(
            trigger=DatabricksRunTrigger(
                conn_id=self.conn_id,
                job_run_id=result.job_run_id,
                task_key=None,
                baseline_attempt=0,
                max_attempts=1,
                poll_interval=self.poll_interval,
                client_factory=self.client_factory,
                repair_id=result.repair_id,
            ),
            method_name="execute_complete",
            kwargs={"launch": result.document()},
            # a timed-out wait fails this try; the retry finds the same run and attaches to it
            timeout=timedelta(seconds=self.wait_timeout_s),
        )
        return None  # defer never returns; this satisfies mypy where Airflow is not installed

    def execute_complete(
        self, context: Context, event: dict[str, Any], launch: dict[str, Any] | None = None
    ) -> dict[str, JsonValue]:
        if event.get("status") != "done":
            raise AirflowException(str(event.get("message") or "the Databricks trigger failed"))
        doc: dict[str, JsonValue] = dict(launch or {})
        doc["snapshot"] = event["snapshot"]
        return doc
