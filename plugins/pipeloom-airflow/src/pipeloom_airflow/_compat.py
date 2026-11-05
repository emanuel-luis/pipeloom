"""The one module of the plugin that imports `airflow` (with `_compat_k8s`). Every name the plugin
uses from Airflow comes from here, so a move between Airflow minors is fixed in one place."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.sdk import DAG, Asset, BaseHook, BaseOperator, BaseSensorOperator, Param, TaskGroup
from airflow.sdk.exceptions import AirflowException, AirflowFailException, AirflowSkipException

if TYPE_CHECKING:
    from airflow.sdk import Context
else:
    Context = Mapping[str, Any]

__all__ = [
    "DAG",
    "AirflowException",
    "AirflowFailException",
    "AirflowSkipException",
    "Asset",
    "BaseHook",
    "BaseOperator",
    "BaseSensorOperator",
    "Context",
    "Param",
    "TaskGroup",
    "aget_connection",
    "airflow_version",
    "get_task_states",
]


def airflow_version() -> str:
    from airflow import __version__

    return str(__version__)


def get_task_states(
    ti: Any, dag_id: str, task_ids: Sequence[str], run_id: str
) -> dict[str, str | None]:
    """Task id -> state of the given tasks in one DAG run, as the task SDK reports them."""
    found = ti.get_task_states(dag_id=dag_id, task_ids=list(task_ids), run_ids=[run_id])
    states = found.get(run_id, {}) if isinstance(found, Mapping) else {}
    return {str(k): (None if v is None else str(v)) for k, v in states.items()}


async def aget_connection(conn_id: str) -> Any:
    """The connection, without blocking the event loop of a trigger."""
    getter = getattr(BaseHook, "aget_connection", None)
    if getter is not None:
        return await getter(conn_id)
    return await asyncio.to_thread(BaseHook.get_connection, conn_id)
