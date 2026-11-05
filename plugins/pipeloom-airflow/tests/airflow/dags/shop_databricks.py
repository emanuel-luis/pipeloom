"""DATABRICKS mode from a manifest: a launch task, one sensor per Databricks task and a finalize
sensor. The test sets PIPELOOM_AF_DBX_PROJECT and _MANIFEST. Parsing calls no workspace."""

import os
from datetime import datetime

from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, ProjectConfig
from pipeloom_airflow.dag import PipeloomDag

dag = PipeloomDag(
    dag_id="shop_databricks",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    project_config=ProjectConfig(
        os.environ["PIPELOOM_AF_DBX_PROJECT"], manifest_path=os.environ["PIPELOOM_AF_DBX_MANIFEST"]
    ),
    target="prod",
    execution_config=ExecutionConfig(
        mode=ExecutionMode.DATABRICKS, databricks_conn_id="pipeloom_databricks"
    ),
)
