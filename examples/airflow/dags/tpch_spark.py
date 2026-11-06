"""TPC-H on spark, one Airflow task per pipeloom unit, run in the worker
(local Spark, so Java comes from packages.txt)."""

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
    dag_id="tpch_spark",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=2,  # each Spark unit starts its own JVM
    project_config=ProjectConfig(project_dir="/usr/local/airflow/examples/tpch_spark"),
    target="dev",
    render_config=RenderConfig(load_method=LoadMode.PROJECT),
    execution_config=ExecutionConfig(mode=ExecutionMode.LOCAL),
)
