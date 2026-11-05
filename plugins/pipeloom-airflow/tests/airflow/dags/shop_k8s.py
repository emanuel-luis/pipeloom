"""KUBERNETES mode from a manifest: units run in pods of one image, paths are the pod's.
The test sets PIPELOOM_AF_PROJECT and _MANIFEST (the parse side). No pod is ever created."""

import os
from datetime import datetime

from pipeloom_airflow.config import (
    ExecutionConfig,
    ExecutionMode,
    KubernetesConfig,
    ProjectConfig,
)
from pipeloom_airflow.dag import PipeloomDag

dag = PipeloomDag(
    dag_id="shop_k8s",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    project_config=ProjectConfig(
        os.environ["PIPELOOM_AF_PROJECT"], manifest_path=os.environ["PIPELOOM_AF_MANIFEST"]
    ),
    target="dev",
    execution_config=ExecutionConfig(
        mode=ExecutionMode.KUBERNETES,
        project_dir="/work/project",
        manifest_path="/work/manifest.json",
        env={"PIPELOOM_ENV": "test"},
        kubernetes=KubernetesConfig(
            image="registry.example/pipeloom:0.3.0",
            namespace="data",
            pod_kwargs={"service_account_name": "pipeloom", "labels": {"team": "data"}},
        ),
    ),
)
