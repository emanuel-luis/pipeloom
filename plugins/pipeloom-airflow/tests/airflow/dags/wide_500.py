"""500 units from a manifest: the parse budget. The test sets PIPELOOM_AF_WIDE_PROJECT and
PIPELOOM_AF_WIDE_MANIFEST."""

import os
from datetime import datetime

from pipeloom_airflow.config import ProjectConfig
from pipeloom_airflow.dag import PipeloomDag

dag = PipeloomDag(
    dag_id="wide_500",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    project_config=ProjectConfig(
        os.environ["PIPELOOM_AF_WIDE_PROJECT"],
        manifest_path=os.environ["PIPELOOM_AF_WIDE_MANIFEST"],
    ),
    target="ci",
)
