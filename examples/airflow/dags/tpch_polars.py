"""TPC-H on polars, one Airflow task per pipeloom unit, run in the worker."""

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
    dag_id="tpch_polars",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    project_config=ProjectConfig(project_dir="/usr/local/airflow/examples/tpch_polars"),
    target="dev",
    render_config=RenderConfig(load_method=LoadMode.PROJECT),
    execution_config=ExecutionConfig(mode=ExecutionMode.LOCAL),
)
