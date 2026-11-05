"""Configuration of the Airflow plugin. Stdlib and the core only; nothing here touches the disk."""

from __future__ import annotations

import ntpath
import posixpath
import string
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final, Literal

from pydantic import JsonValue

from pipeloom.diagnostics import Report
from pipeloom_airflow.naming import PLACEHOLDERS


class LoadMode(StrEnum):
    MANIFEST = "manifest"
    PROJECT = "project"


class ExecutionMode(StrEnum):
    LOCAL = "local"
    VIRTUALENV = "virtualenv"
    KUBERNETES = "kubernetes"
    DATABRICKS = "databricks"
    DOCKER = "docker"


@dataclass(frozen=True)
class ProjectConfig:
    """Where the DAG file reads the project."""

    project_dir: str
    manifest_path: str | None = None
    vars: Mapping[str, JsonValue] = field(default_factory=dict)
    cache_dir: str | None = None


@dataclass(frozen=True)
class RenderConfig:
    load_method: LoadMode = LoadMode.MANIFEST
    select: tuple[str, ...] = ()
    exclude: tuple[str, ...] = ()
    group_by: Literal["none", "folder", "tag"] = "none"
    """Visual TaskGroups only; every unit is still one task."""
    emit_assets: bool = False
    asset_uri: str = "pipeloom://{project}/{target}/{relation}"


@dataclass(frozen=True)
class KubernetesConfig:
    image: str
    namespace: str | None = None
    python: str = "python"
    pod_kwargs: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExecutionConfig:
    mode: ExecutionMode = ExecutionMode.LOCAL
    project_dir: str | None = None
    manifest_path: str | None = None
    """Where units execute; the defaults are ProjectConfig's."""
    virtualenv_dir: str | None = None
    kubernetes: KubernetesConfig | None = None
    env: Mapping[str, str] = field(default_factory=dict)
    databricks_conn_id: str = "databricks_default"
    databricks_pipeline: str = "main"
    databricks_ensure_job: bool = False
    databricks_poll: Literal["per_task", "shared"] = "per_task"
    databricks_poll_interval: float = 30.0
    databricks_client_factory: str | None = None
    """`module:attr`, for tests only."""


POD_KWARGS_RESERVED: Final = (
    "cmds",
    "arguments",
    "image",
    "name",
    "task_id",
    "deferrable",
    "do_xcom_push",
)
OPERATOR_ARGS_RESERVED: Final = (
    "task_id",
    "retries",
    "retry_delay",
    "trigger_rule",
    "outlets",
    "execution_timeout",
    "deferrable",
)


def _absolute(path: str) -> bool:
    """Absolute on either platform: the worker that runs a unit may not be the parser's OS."""
    return posixpath.isabs(path) or ntpath.isabs(path)


def validate_configs(
    project: ProjectConfig,
    render: RenderConfig,
    execution: ExecutionConfig,
    operator_args: Mapping[str, Any],
    report: Report,
) -> None:
    """Report PLA001 and PLA002. Strings only: execution-side paths are never touched."""

    def bad(message: str, hint: str | None = None) -> None:
        report.add("PLA002", message, hint=hint)

    if execution.mode is ExecutionMode.DOCKER:
        report.add(
            "PLA001",
            "execution mode 'docker' is not supported",
            hint="use local, virtualenv, kubernetes or databricks",
        )
    if render.load_method is LoadMode.MANIFEST and not project.manifest_path:
        bad("load_method 'manifest' needs project.manifest_path", "or use load_method 'project'")
    paths = {
        "project.project_dir": project.project_dir,
        "project.manifest_path": project.manifest_path,
        "project.cache_dir": project.cache_dir,
        "execution.project_dir": execution.project_dir,
        "execution.manifest_path": execution.manifest_path,
        "execution.virtualenv_dir": execution.virtualenv_dir,
    }
    for name, value in paths.items():
        if value is not None and not _absolute(value):
            bad(f"{name} must be an absolute path, got '{value}'")
    if execution.mode is ExecutionMode.VIRTUALENV and not execution.virtualenv_dir:
        bad("execution mode 'virtualenv' needs execution.virtualenv_dir")
    if execution.mode is ExecutionMode.KUBERNETES:
        if execution.kubernetes is None or not execution.kubernetes.image:
            bad("execution mode 'kubernetes' needs execution.kubernetes.image")
        else:
            for key in sorted(set(execution.kubernetes.pod_kwargs) & set(POD_KWARGS_RESERVED)):
                bad(f"pod_kwargs.{key} is set by pipeloom", "remove it from pod_kwargs")
    for key in sorted(set(operator_args) & set(OPERATOR_ARGS_RESERVED)):
        bad(f"operator_args.{key} is set by pipeloom", "remove it from operator_args")
    try:
        fields = {f for _, f, _, _ in string.Formatter().parse(render.asset_uri) if f is not None}
    except ValueError as err:
        bad(f"asset_uri is not a valid template: {err}")
        return
    for name in sorted(fields - set(PLACEHOLDERS)):
        bad(f"asset_uri has an unknown placeholder '{{{name}}}'", f"use: {', '.join(PLACEHOLDERS)}")
