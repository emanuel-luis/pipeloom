"""LOCAL mode from a manifest, with assets. The test sets PIPELOOM_AF_PROJECT and _MANIFEST."""

import os
from datetime import datetime

from pipeloom_airflow.config import ProjectConfig, RenderConfig
from pipeloom_airflow.dag import PipeloomDag

dag = PipeloomDag(
    dag_id="shop_local",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    project_config=ProjectConfig(
        os.environ["PIPELOOM_AF_PROJECT"], manifest_path=os.environ["PIPELOOM_AF_MANIFEST"]
    ),
    target="dev",
    render_config=RenderConfig(emit_assets=True),
)
