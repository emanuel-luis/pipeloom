"""Apache Airflow renderer for pipeloom.

Importing the package imports nothing: names load on first use (PEP 562). Only the modules
`_compat` and `_compat_k8s`, which arrive with the DAG classes, may import `airflow`.
"""

from __future__ import annotations

import importlib
from typing import Any

_LAZY = {
    "ExecutionConfig": "config",
    "ExecutionMode": "config",
    "KubernetesConfig": "config",
    "LoadMode": "config",
    "ProjectConfig": "config",
    "RenderConfig": "config",
    "validate_configs": "config",
    "PipeloomAirflowError": "errors",
    "LoadedPlan": "load",
    "load_plan": "load",
    "TaskPlan": "render",
    "UnitTask": "render",
    "render_plan": "render",
}

__all__ = list(_LAZY)


def __getattr__(name: str) -> Any:
    module = _LAZY.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(importlib.import_module(f"{__name__}.{module}"), name)
