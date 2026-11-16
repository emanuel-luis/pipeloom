"""TPC-H on polars in WATCHER mode: one producer task runs the whole selection, and one deferrable
watcher per model reports its result and declares its asset. Needs a stable manifest, see README."""

from datetime import datetime

from pipeloom_airflow.config import (
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProjectConfig,
    RenderConfig,
)
from pipeloom_airflow.dag import PipeloomDag

PROJECT = "/usr/local/airflow/examples/tpch_polars"

dag = PipeloomDag(
    dag_id="tpch_polars_watcher",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    project_config=ProjectConfig(
        project_dir=PROJECT,
        manifest_path=f"{PROJECT}/target/manifest.json",
    ),
    target="dev",
    render_config=RenderConfig(load_method=LoadMode.MANIFEST, emit_assets=True),
    execution_config=ExecutionConfig(mode=ExecutionMode.WATCHER, watcher_workers=4),
)
