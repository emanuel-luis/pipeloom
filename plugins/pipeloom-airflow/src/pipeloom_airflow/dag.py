"""A pipeloom plan as an Airflow DAG or task group: one task per unit and a finalize task."""

from __future__ import annotations

import importlib.util
import json
import logging
import os
from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from pipeloom.diagnostics import Report, Severity, format_diagnostic
from pipeloom_airflow import naming
from pipeloom_airflow._compat import DAG, Asset, BaseOperator, Param, TaskGroup, airflow_version
from pipeloom_airflow.commands import CoreCommand
from pipeloom_airflow.config import (
    ExecutionConfig,
    ExecutionMode,
    LoadMode,
    ProjectConfig,
    RenderConfig,
    validate_configs,
)
from pipeloom_airflow.errors import PipeloomAirflowError
from pipeloom_airflow.load import load_plan
from pipeloom_airflow.operators.local import PipeloomFinalizeOperator, PipeloomRunOperator
from pipeloom_airflow.render import TaskPlan, render_plan
from pipeloom_airflow.version import check_airflow_version

logger = logging.getLogger("pipeloom_airflow")

EXECUTION_TIMEOUT_SLACK_S = 300.0
"""Added to a unit's timeout and cancel grace, so the core times a unit out before Airflow."""


def _k8s_provider_installed() -> bool:
    try:
        return importlib.util.find_spec("airflow.providers.cncf.kubernetes") is not None
    except ImportError:
        return False


def _pod_kwargs(execution: ExecutionConfig, plan: TaskPlan) -> dict[str, Any]:
    """Keyword arguments every pod gets: the image, the namespace, the user's `pod_kwargs`, the
    execution env as pod env, and the cancel grace as the pod's termination grace."""
    from pipeloom_airflow._compat_k8s import merge_env_vars

    k8s = execution.kubernetes
    assert k8s is not None  # validate_configs reports PLA002 otherwise
    kwargs: dict[str, Any] = {"image": k8s.image, **k8s.pod_kwargs}
    if k8s.namespace is not None:
        kwargs.setdefault("namespace", k8s.namespace)
    kwargs.setdefault("termination_grace_period", int(plan.cancel_grace_s))
    env_vars = merge_env_vars(execution.env, kwargs.get("env_vars"))
    if env_vars:
        kwargs["env_vars"] = env_vars
    return kwargs


def _plan(
    project: ProjectConfig,
    target: str,
    render: RenderConfig,
    execution: ExecutionConfig,
    operator_args: Mapping[str, Any],
) -> tuple[TaskPlan, CoreCommand]:
    """Version check, configs, plan, tasks. Errors raise `PipeloomAirflowError`, which Airflow
    shows as the DAG file's import error; warnings go to the `pipeloom_airflow` logger."""
    report = Report()
    task_plan = None
    if check_airflow_version(airflow_version(), report):
        validate_configs(project, render, execution, operator_args, report)
        if execution.mode is ExecutionMode.KUBERNETES and not _k8s_provider_installed():
            report.add(
                "PLA001",
                "execution mode 'kubernetes' needs the cncf-kubernetes provider",
                hint="install pipeloom-airflow[kubernetes]",
            )
    if not report.has_errors:
        loaded = load_plan(project, render, target, report, env=os.environ)
        if loaded is not None and not report.has_errors:
            task_plan = render_plan(loaded, target, render, execution, report)
    for d in report.sorted():
        if d.severity is not Severity.ERROR:
            logger.warning("%s", format_diagnostic(d))
    if report.has_errors or task_plan is None:
        raise PipeloomAirflowError(d for d in report.sorted() if d.severity is Severity.ERROR)
    manifest = None
    if render.load_method is LoadMode.MANIFEST:
        manifest = execution.manifest_path or project.manifest_path
    command = CoreCommand(
        project_dir=execution.project_dir or project.project_dir,
        target=target,
        manifest=manifest,
        plan_hash=task_plan.plan_hash,
        vars_json=json.dumps(dict(project.vars), sort_keys=True) if project.vars else None,
    )
    return task_plan, command


def build_tasks(
    plan: TaskPlan,
    execution: ExecutionConfig,
    command: CoreCommand,
    *,
    group_id: str | None,
    operator_args: Mapping[str, Any],
) -> dict[str, BaseOperator]:
    """Create the operators in the current DAG (and task group) context: rendered task id ->
    operator, plus `pipeloom_finalize`. Visual groups nest as `UnitTask.group` says."""
    if execution.mode is ExecutionMode.DATABRICKS:
        return _build_databricks(
            plan, execution, command, group_id=group_id, operator_args=operator_args
        )
    venv = execution.virtualenv_dir if execution.mode is ExecutionMode.VIRTUALENV else None
    pod: dict[str, Any] | None = None
    python = "python"
    if execution.mode is ExecutionMode.KUBERNETES:
        # imported here: the provider is needed only in this mode
        from pipeloom_airflow.operators.kubernetes import (
            PipeloomKubernetesFinalizeOperator,
            PipeloomKubernetesOperator,
        )

        pod = _pod_kwargs(execution, plan)
        assert execution.kubernetes is not None
        python = execution.kubernetes.python
    groups: dict[tuple[str, ...], Any] = {}

    def group(path: tuple[str, ...]) -> Any:
        if not path:
            return None
        if path not in groups:
            # no parent_group for a top-level group: the default is the enclosing group
            parent = {"parent_group": group(path[:-1])} if len(path) > 1 else {}
            groups[path] = TaskGroup(group_id=path[-1], **parent)
        return groups[path]

    ops: dict[str, BaseOperator] = {}
    for t in plan.tasks:
        timeout = None
        if t.timeout_s is not None:
            timeout = timedelta(
                seconds=t.timeout_s + plan.cancel_grace_s + EXECUTION_TIMEOUT_SLACK_S
            )
        extra: dict[str, Any] = {}
        if t.assets:
            extra["outlets"] = [Asset(name=uri, uri=uri) for uri in t.assets]
        if t.group:
            extra["task_group"] = group(t.group)
        common: dict[str, Any] = {
            "task_id": t.task_id,
            "retries": t.retries,
            "retry_delay": timedelta(seconds=t.retry_delay_s),
            "execution_timeout": timeout,
            **extra,
            **operator_args,
        }
        if pod is not None:
            ops[t.task_id] = PipeloomKubernetesOperator(
                unit_id=t.unit_id,
                command=command,
                python=python,
                group_id=group_id,
                **pod,
                **common,
            )
        else:
            ops[t.task_id] = PipeloomRunOperator(
                unit_id=t.unit_id,
                command=command,
                venv_dir=venv,
                env=execution.env,
                retry_on=t.retry_on,
                cancel_grace_s=plan.cancel_grace_s,
                group_id=group_id,
                **common,
            )
    for t in plan.tasks:
        for up in t.upstream:
            ops[up] >> ops[t.task_id]
    task_units = {ops[t.task_id].task_id: t.units for t in plan.tasks}
    finalize: BaseOperator
    if pod is not None:
        finalize = PipeloomKubernetesFinalizeOperator(
            task_id=naming.FINALIZE_ID,
            command=command,
            python=python,
            task_units=task_units,
            select=plan.select,
            exclude=plan.exclude,
            group_id=group_id,
            **pod,
            **operator_args,
        )
    else:
        finalize = PipeloomFinalizeOperator(
            task_id=naming.FINALIZE_ID,
            command=command,
            venv_dir=venv,
            env=execution.env,
            task_units=task_units,
            select=plan.select,
            exclude=plan.exclude,
            group_id=group_id,
            cancel_grace_s=plan.cancel_grace_s,
            **operator_args,
        )
    for leaf in plan.leaves:
        ops[leaf] >> finalize
    ops[naming.FINALIZE_ID] = finalize
    return ops


def _build_databricks(
    plan: TaskPlan,
    execution: ExecutionConfig,
    command: CoreCommand,
    *,
    group_id: str | None,
    operator_args: Mapping[str, Any],
) -> dict[str, BaseOperator]:
    """`pipeloom_launch` -> one sensor per Databricks task (plan edges) -> `pipeloom_finalize`,
    a sensor on the job's finalize task that fails when the pipeloom run failed."""
    # imported here: pipeloom-databricks is needed only in this mode
    from pipeloom_airflow.databricks.launch import PipeloomDatabricksRunOperator
    from pipeloom_airflow.databricks.sensor import PipeloomDatabricksTaskSensor
    from pipeloom_databricks.api import FINALIZE_KEY

    launch = PipeloomDatabricksRunOperator(
        task_id=naming.LAUNCH_ID,
        command=command,
        pipeline=execution.databricks_pipeline,
        conn_id=execution.databricks_conn_id,
        ensure_job=execution.databricks_ensure_job,
        select=plan.select,
        exclude=plan.exclude,
        poll_interval=execution.databricks_poll_interval,
        client_factory=execution.databricks_client_factory,
        group_id=group_id,
        poll=execution.databricks_poll,
        **operator_args,
    )
    sensing: dict[str, Any] = {
        "poll": execution.databricks_poll,
        "conn_id": execution.databricks_conn_id,
        "poll_interval": execution.databricks_poll_interval,
        "client_factory": execution.databricks_client_factory,
    }
    ops: dict[str, BaseOperator] = {naming.LAUNCH_ID: launch}
    for t in plan.tasks:
        assert t.databricks_key is not None
        extra: dict[str, Any] = {}
        if t.assets:
            extra["outlets"] = [Asset(name=uri, uri=uri) for uri in t.assets]
        ops[t.task_id] = PipeloomDatabricksTaskSensor(
            task_id=t.task_id,
            task_key=t.databricks_key,
            launch_task_id=launch.task_id,
            max_attempts=t.max_attempts,
            **sensing,
            **extra,
            **operator_args,
        )
    for t in plan.tasks:
        if not t.upstream:
            launch >> ops[t.task_id]
        for up in t.upstream:
            ops[up] >> ops[t.task_id]
    finalize = PipeloomDatabricksTaskSensor(
        task_id=naming.FINALIZE_ID,
        task_key=FINALIZE_KEY,
        launch_task_id=launch.task_id,
        max_attempts=1,
        trigger_rule="all_done",
        **sensing,
        **operator_args,
    )
    launch >> finalize
    for leaf in plan.leaves:
        ops[leaf] >> finalize
    ops[naming.FINALIZE_ID] = finalize
    return ops


class PipeloomDag(DAG):
    """A DAG built from a pipeloom project. Adds the param `full_refresh` (False) unless the
    caller defines it; every other keyword goes to `DAG`."""

    def __init__(
        self,
        *,
        project_config: ProjectConfig,
        target: str,
        render_config: RenderConfig = RenderConfig(),  # noqa: B008 - frozen dataclass
        execution_config: ExecutionConfig = ExecutionConfig(),  # noqa: B008 - frozen dataclass
        operator_args: Mapping[str, Any] | None = None,
        **dag_kwargs: Any,
    ) -> None:
        args = dict(operator_args or {})
        plan, command = _plan(project_config, target, render_config, execution_config, args)
        params = dict(dag_kwargs.pop("params", None) or {})
        params.setdefault("full_refresh", Param(False, type="boolean"))
        super().__init__(params=params, **dag_kwargs)
        with self:
            build_tasks(plan, execution_config, command, group_id=None, operator_args=args)


class PipeloomTaskGroup(TaskGroup):
    """A task group built from a pipeloom project inside an existing DAG. Its run id includes
    the group id, so two groups in one DAG are two pipeloom runs."""

    def __init__(
        self,
        group_id: str,
        *,
        project_config: ProjectConfig,
        target: str,
        render_config: RenderConfig = RenderConfig(),  # noqa: B008 - frozen dataclass
        execution_config: ExecutionConfig = ExecutionConfig(),  # noqa: B008 - frozen dataclass
        operator_args: Mapping[str, Any] | None = None,
        **group_kwargs: Any,
    ) -> None:
        args = dict(operator_args or {})
        plan, command = _plan(project_config, target, render_config, execution_config, args)
        super().__init__(group_id=group_id, **group_kwargs)
        with self:
            build_tasks(plan, execution_config, command, group_id=self.group_id, operator_args=args)
