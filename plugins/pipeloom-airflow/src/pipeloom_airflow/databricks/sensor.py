"""One sensor per Databricks task: it mirrors the task's result from the launch's snapshot."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pipeloom_airflow._compat import (
    AirflowException,
    AirflowFailException,
    AirflowSkipException,
    BaseSensorOperator,
    Context,
)
from pipeloom_airflow.databricks.mirror import Verdict, run_from_snapshot, task_verdict


class PipeloomDatabricksTaskSensor(BaseSensorOperator):
    """Succeeds, fails or skips as Databricks task `task_key` did. It reads the snapshot the
    launch task `launch_task_id` stored, so it makes no API call (`shared` poll mode)."""

    def __init__(
        self, *, task_key: str, launch_task_id: str, max_attempts: int, **kwargs: Any
    ) -> None:
        kwargs.setdefault("retries", 0)
        super().__init__(**kwargs)
        self.task_key = task_key
        self.launch_task_id = launch_task_id
        self.max_attempts = max_attempts

    def poke(self, context: Context) -> bool:  # pragma: no cover - execute decides at once
        return True

    def execute(self, context: Context) -> None:
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
