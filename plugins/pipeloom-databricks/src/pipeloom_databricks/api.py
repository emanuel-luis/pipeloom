"""The stable surface other pipeloom distributions (such as an Airflow renderer) may call.

Importing it loads no SDK: `connect` and `connect_explicit` import `databricks.sdk` only when they
run.
"""

from __future__ import annotations

from typing import Literal

from pipeloom_databricks.bundle import Runtime
from pipeloom_databricks.client import (
    ApiError,
    ConnectionNotConfigured,
    DatabricksClient,
    JobSummary,
    RunInfo,
    TaskRunInfo,
    connect,
)
from pipeloom_databricks.deploy import DeployResult, DuplicateJobs, api_errors, deploy, find_job
from pipeloom_databricks.finalize_states import task_states
from pipeloom_databricks.grouping import task_key_map
from pipeloom_databricks.keys import FINALIZE_KEY
from pipeloom_databricks.render import deploy_options, job_name, render_job
from pipeloom_databricks.repair import repair, rerun_set
from pipeloom_databricks.run import TERMINAL, RunStarted, SelectionError, find_run, run_job

__all__ = [
    "FINALIZE_KEY",
    "TERMINAL",
    "ApiError",
    "ConnectionNotConfigured",
    "DatabricksClient",
    "DeployResult",
    "DuplicateJobs",
    "JobSummary",
    "RunInfo",
    "RunStarted",
    "Runtime",
    "SelectionError",
    "TaskRunInfo",
    "api_errors",
    "connect",
    "connect_explicit",
    "deploy",
    "deploy_options",
    "find_job",
    "find_run",
    "job_name",
    "render_job",
    "repair",
    "rerun_set",
    "run_job",
    "task_key_map",
    "task_states",
]


def connect_explicit(
    *,
    host: str,
    auth_type: Literal["pat", "oauth-m2m"],
    token: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> DatabricksClient:
    """A client from explicit credentials only; see `client.connect.connect_explicit`."""
    from pipeloom_databricks.client.connect import connect_explicit as _connect

    return _connect(
        host=host,
        auth_type=auth_type,
        token=token,
        client_id=client_id,
        client_secret=client_secret,
    )
