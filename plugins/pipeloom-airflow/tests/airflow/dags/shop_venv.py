"""VIRTUALENV mode, compiling the project (PROJECT load) into a cache under the test's temp dir."""

import os
import sys
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
    dag_id="shop_venv",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    project_config=ProjectConfig(
        os.environ["PIPELOOM_AF_PROJECT"], cache_dir=os.environ["PIPELOOM_AF_CACHE"]
    ),
    target="dev",
    render_config=RenderConfig(load_method=LoadMode.PROJECT),
    execution_config=ExecutionConfig(mode=ExecutionMode.VIRTUALENV, virtualenv_dir=sys.prefix),
)
