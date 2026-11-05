"""The only module besides `_compat` that imports the cncf-kubernetes provider (and the
`kubernetes` client it brings). Import it only when the extra `kubernetes` is installed."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

__all__ = ["KubernetesPodOperator", "merge_env_vars", "pod_name"]

POD_NAME_PREFIX = "pipeloom-"
POD_NAME_BASE_MAX = 54
"""The provider adds `-` and 8 random characters to a name, and a pod name is 63 at most."""


def pod_name(task_id: str) -> str:
    """`pipeloom-<task id>` as a DNS-1123 label: lower case, `[a-z0-9-]`, at most 54 characters."""
    name = re.sub(r"[^a-z0-9-]+", "-", f"{POD_NAME_PREFIX}{task_id}".lower())
    return name[:POD_NAME_BASE_MAX].strip("-")


def merge_env_vars(env: Mapping[str, str], existing: Any) -> Any:
    """`ExecutionConfig.env` plus the `env_vars` the user put in `pod_kwargs` (a dict or a list of
    `V1EnvVar`); the pod_kwargs entry wins. Returns what `KubernetesPodOperator(env_vars=)` takes."""
    if not env:
        return existing
    if not existing:
        return dict(env)
    if isinstance(existing, Mapping):
        return {**env, **existing}
    named = {v.name for v in existing}
    return [*(k8s.V1EnvVar(name=k, value=v) for k, v in env.items() if k not in named), *existing]
