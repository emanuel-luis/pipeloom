"""The stable surface other pipeloom distributions (such as an Airflow renderer) may call.

Importing it loads no SDK: `connect` imports `databricks.sdk` only when it runs.
"""

from __future__ import annotations

from pipeloom_databricks.client import (
    ApiError,
    ConnectionNotConfigured,
    DatabricksClient,
    JobSummary,
    RunInfo,
    TaskRunInfo,
    connect,
)
from pipeloom_databricks.deploy import DeployResult, DuplicateJobs, deploy, find_job
from pipeloom_databricks.grouping import task_key_map
from pipeloom_databricks.render import deploy_options, render_job

__all__ = [
    "ApiError",
    "ConnectionNotConfigured",
    "DatabricksClient",
    "DeployResult",
    "DuplicateJobs",
    "JobSummary",
    "RunInfo",
    "TaskRunInfo",
    "connect",
    "deploy",
    "deploy_options",
    "find_job",
    "render_job",
    "task_key_map",
]
