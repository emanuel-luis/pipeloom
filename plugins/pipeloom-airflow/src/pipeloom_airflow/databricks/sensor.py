"""One sensor per Databricks task: it mirrors the task's result from the launch's snapshot."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Literal

from pipeloom.diagnostics import Report, format_diagnostic
from pipeloom_airflow._compat import (
    AirflowException,
    AirflowFailException,
    AirflowSkipException,
    BaseSensorOperator,
    Context,
)
from pipeloom_airflow.databricks.launch import client_for
from pipeloom_airflow.databricks.mirror import Verdict, run_from_snapshot, task_verdict
from pipeloom_airflow.databricks.triggers import DatabricksRunTrigger
from pipeloom_databricks.api import ApiError


class PipeloomDatabricksTaskSensor(BaseSensorOperator):
    """Succeeds, fails or skips as Databricks task `task_key` did.

    `shared` poll mode: it reads the snapshot the launch task `launch_task_id` stored and makes no
    API call. `per_task` mode: it makes one `get_run` call and, without a verdict, defers on a
    trigger for its own task key until `timeout` (24 hours by default).
    """

    def __init__(
        self,
        *,
        task_key: str,
        launch_task_id: str,
        max_attempts: int,
        poll: Literal["per_task", "shared"] = "shared",
        conn_id: str = "databricks_default",
        poll_interval: float = 30.0,
        client_factory: str | None = None,
        **kwargs: Any,
    ) -> None:
        kwargs.setdefault("retries", 0)
        kwargs.setdefault("timeout", 24 * 3600.0)
        super().__init__(**kwargs)
        self.task_key = task_key
        self.launch_task_id = launch_task_id
        self.max_attempts = max_attempts
        self.poll = poll
        self.conn_id = conn_id
        self.poll_interval = poll_interval
        self.client_factory = client_factory

    def poke(self, context: Context) -> bool:  # pragma: no cover - execute decides at once
        return True

    def execute(self, context: Context) -> None:
        if self.poll == "per_task":
            self._execute_per_task(context)
            return
        launch = context["ti"].xcom_pull(task_ids=self.launch_task_id)
        if not isinstance(launch, Mapping) or not isinstance(launch.get("snapshot"), Mapping):
            raise AirflowFailException(
                f"the launch task {self.launch_task_id} left no snapshot of the job run"
            )
        run = run_from_snapshot(launch["snapshot"])
        baseline = dict(launch.get("baseline") or {}).get(self.task_key, 0)
        verdict = task_verdict(
            run, self.task_key, baseline_attempt=int(baseline), max_attempts=self.max_attempts
        )
        if verdict is None:
            raise AirflowFailException(
                f"job run {run.run_id} is {run.life_cycle_state}; task {self.task_key} is open"
            )
        self._settle(verdict, run.run_page_url)

    def _execute_per_task(self, context: Context) -> None:
        launch = context["ti"].xcom_pull(task_ids=self.launch_task_id)
        if not isinstance(launch, Mapping) or not isinstance(launch.get("job_run_id"), int):
            raise AirflowFailException(
                f"the launch task {self.launch_task_id} left no job run to watch"
            )
        job_run_id: int = launch["job_run_id"]
        baseline = int(dict(launch.get("baseline") or {}).get(self.task_key, 0))
        report = Report()
        client = client_for(self.conn_id, self.client_factory, report)
        if client is None:
            raise AirflowFailException("\n".join(format_diagnostic(d) for d in report.sorted()))
        try:
            run = client.get_run(job_run_id)
        except ApiError as err:
            raise AirflowException(
                f"PLD009 Databricks API request failed (HTTP {err.status}): {err}"
            ) from err
        verdict = task_verdict(
            run, self.task_key, baseline_attempt=baseline, max_attempts=self.max_attempts
        )
        if verdict is not None:
            self._settle(verdict, run.run_page_url)
            return
        self.defer(
            trigger=DatabricksRunTrigger(
                conn_id=self.conn_id,
                job_run_id=job_run_id,
                task_key=self.task_key,
                baseline_attempt=baseline,
                max_attempts=self.max_attempts,
                poll_interval=self.poll_interval,
                client_factory=self.client_factory,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout),
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        if event.get("status") != "done":
            raise AirflowException(str(event.get("message") or "the Databricks trigger failed"))
        verdict = Verdict(event["outcome"], int(event["attempt"]), event.get("message"))
        self._settle(verdict, event.get("run_page_url"))

    def _settle(self, verdict: Verdict, url: str | None) -> None:
        text = verdict.message or f"task {self.task_key} {verdict.outcome}"
        if url:
            text = f"{text} ({url})"
        if verdict.outcome == "failed":
            raise AirflowFailException(text)
        if verdict.outcome == "skipped":
            raise AirflowSkipException(text)
        self.log.info("%s", text)
