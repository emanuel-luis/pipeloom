"""TPC-H on Databricks: runs the job deployed with `pipeloom databricks deploy --target dev`."""

from datetime import datetime

from pipeloom_airflow.config import (
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProjectConfig,
    RenderConfig,
)
from pipeloom_airflow.dag import PipeloomDag

dag = PipeloomDag(
    dag_id="tpch_databricks",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    project_config=ProjectConfig(project_dir="/usr/local/airflow/examples/tpch_databricks"),
    target="dev",
    render_config=RenderConfig(load_method=LoadMode.PROJECT),
    execution_config=ExecutionConfig(
        mode=ExecutionMode.DATABRICKS,
        databricks_conn_id="databricks_default",
        databricks_pipeline="main",
    ),
)
