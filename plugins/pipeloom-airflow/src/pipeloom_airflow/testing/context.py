"""A stand-in for the Airflow task context, so operators run in a test without a scheduler or a
metadata database. Pure: imports no Airflow."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from typing import Any


@dataclass
class FakeTaskInstance:
    """The parts of the runtime task instance the operators use."""

    dag_id: str
    run_id: str
    task_id: str
    try_number: int = 1
    xcoms: dict[tuple[str, str], Any] = field(default_factory=dict)
    """(task id, key) -> value."""
    states: dict[str, str | None] = field(default_factory=dict)
    """Task id -> state, as `get_task_states` returns for this run."""

    def xcom_push(self, key: str, value: Any) -> None:
        self.xcoms[(self.task_id, key)] = value

    def xcom_pull(self, task_ids: str | None = None, *, key: str = "return_value") -> Any:
        return self.xcoms.get((task_ids or self.task_id, key))

    def get_task_states(
        self,
        dag_id: str,
        task_ids: Sequence[str] | None = None,
        run_ids: Sequence[str] | None = None,
        **_: Any,
    ) -> dict[str, dict[str, str | None]]:
        wanted = self.states if task_ids is None else {t: self.states.get(t) for t in task_ids}
        return {r: dict(wanted) for r in (run_ids or [self.run_id])}


def make_context(
    ti: FakeTaskInstance, *, run_after: datetime, params: Mapping[str, Any]
) -> dict[str, Any]:
    """A context dict with `ti`, `dag_run` (`run_id`, `run_after`), `run_id` and `params`."""
    dag_run = SimpleNamespace(dag_id=ti.dag_id, run_id=ti.run_id, run_after=run_after)
    return {"ti": ti, "dag_run": dag_run, "run_id": ti.run_id, "params": dict(params)}
