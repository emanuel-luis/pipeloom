"""A pipeloom task group inside a plain DAG, between two other tasks, with folder groups."""

import os
from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from pipeloom_airflow.config import ProjectConfig, RenderConfig
from pipeloom_airflow.dag import PipeloomTaskGroup

with DAG(dag_id="shop_group", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
    shop = PipeloomTaskGroup(
        "shop",
        project_config=ProjectConfig(
            os.environ["PIPELOOM_AF_PROJECT"], manifest_path=os.environ["PIPELOOM_AF_MANIFEST"]
        ),
        target="dev",
        render_config=RenderConfig(group_by="tag"),
    )
    EmptyOperator(task_id="start") >> shop >> EmptyOperator(task_id="end")
