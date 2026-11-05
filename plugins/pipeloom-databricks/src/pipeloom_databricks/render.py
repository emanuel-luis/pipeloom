"""Render a plan as a job (create, reset, bundle) or as a one-time run (runs/submit).

Pure: the same plan and options give the same settings, and nothing here reaches a workspace.
Problems go to the `Report` (PLD001, PLD003, PLD004, PLD005) and the function returns None.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Final

from pydantic import JsonValue, ValidationError

from pipeloom.diagnostics import Report
from pipeloom.plan.manifest import canonical_json
from pipeloom.plan.model import Plan
from pipeloom_databricks import keys
from pipeloom_databricks.grouping import TaskUnit, build_tasks
from pipeloom_databricks.jobs_model import (
    EnvironmentSpec,
    JobCluster,
    JobEnvironment,
    JobParameter,
    JobSettings,
    PythonWheelTask,
    QueueSettings,
    SubmitSpec,
    Task,
    TaskDependency,
    to_json,
)
from pipeloom_databricks.options import (
    DatabricksDeployOptions,
    JobClusterCompute,
    ServerlessCompute,
)

PACKAGE_NAME: Final = "pipeloom_databricks"
ENTRY_POINT: Final = "pipeloom_databricks_task"
OWNED_SETTINGS: Final = (
    "tasks",
    "job_clusters",
    "environments",
    "parameters",
    "max_concurrent_runs",
    "queue",
    "format",
    "git_source",
    "deployment",
    "name",
)
"""Job keys pipeloom sets itself. `settings` may not carry them."""
TAG_PREFIX: Final = "pipeloom_"
MAX_ENVIRONMENTS: Final = 10
FINALIZE_TIMEOUT: Final = 1800
TIMEOUT_MARGIN: Final = 300
_ABSOLUTE = re.compile(r"^(/|[A-Za-z][A-Za-z0-9+.-]+:/)")


@dataclass(frozen=True)
class Locations:
    """Where the rendered tasks find the project, and what the target says about the runtime."""

    project_root: str
    """The project directory as the task sees it (a workspace or volume path)."""
    manifest_path: str
    """The stable manifest shipped with the project, as the task sees it."""
    artifacts_root: str | None
    """The target's `artifacts.root`; PLD004 when unset or relative."""
    state_shared: bool
    """Whether the target's state store is shared between machines; PLD004 when not."""
    cancel_grace: int = 30
    """The target's `cancel_grace` in seconds, added to a model timeout."""


@dataclass(frozen=True)
class RenderedJob:
    settings: JobSettings
    job_tag: str
    """The value of the `pipeloom_job` tag: `<project>.<pipeline>.<target>`."""
    settings_hash: str
    tasks: tuple[TaskUnit, ...]


@dataclass(frozen=True)
class _Parts:
    opts: DatabricksDeployOptions
    units: tuple[TaskUnit, ...]
    tasks: tuple[Task, ...]
    environments: tuple[JobEnvironment, ...]
    clusters: tuple[JobCluster, ...]


@dataclass(frozen=True)
class _Run:
    """The arguments that carry the run, as a job and as a one-time run spell them."""

    run_id: str
    full_refresh: str
    job: tuple[str, ...]
    start_ms: str


_JOB_RUN: Final = _Run(
    "{{job.parameters.pipeloom_run_id}}",
    "{{job.parameters.full_refresh}}",
    ("--job-id={{job.id}}", "--job-run-id={{job.run_id}}"),
    "{{job.start_time.timestamp_ms}}",
)


def deploy_options(plan: Plan, report: Report) -> DatabricksDeployOptions | None:
    """The plan's deploy options, or None with PLD004."""
    raw = plan.metadata.deploy
    if raw is None or raw.get("kind") != "databricks":
        report.add("PLD004", "the target has no databricks deploy block")
        return None
    if "${secret:" in json.dumps(raw):
        report.add(
            "PLD004",
            "the deploy block holds a ${secret:...} reference, which a rendered job cannot resolve",
            hint="put the secret in the project's plugins or the job's own configuration",
        )
        return None
    try:
        return DatabricksDeployOptions.model_validate({k: v for k, v in raw.items() if k != "kind"})
    except ValidationError as err:
        report.add("PLD004", f"invalid deploy options: {err.errors()[0]['msg']}")
        return None


def settings_hash(settings: Mapping[str, JsonValue]) -> str:
    """sha256 of the canonical settings without the `pipeloom_settings_hash` tag."""
    doc = dict(settings)
    tags = doc.get("tags")
    if isinstance(tags, dict):
        doc["tags"] = {k: v for k, v in tags.items() if k != "pipeloom_settings_hash"}
    return hashlib.sha256(canonical_json(doc)).hexdigest()


def job_name(plan: Plan, pipeline: str) -> str:
    return f"pipeloom_{plan.metadata.project}_{pipeline}_{plan.metadata.target}"


def _check_options(opts: DatabricksDeployOptions, locs: Locations, report: Report) -> None:
    def bad(message: str, hint: str | None = None) -> None:
        report.add("PLD004", message, hint=hint)

    for name, compute in sorted(opts.compute.items()):
        if isinstance(compute, ServerlessCompute) and compute.environment_version == "1":
            bad(
                f"compute '{name}' uses serverless environment version 1 (Python 3.10)",
                "use environment_version 2 or later",
            )
    wanted = [opts.default_compute, *(r.compute for r in opts.compute_rules)]
    for name in sorted({n for n in wanted if n not in opts.compute}):
        bad(f"compute '{name}' is not defined", f"compute: {', '.join(sorted(opts.compute))}")
    for key in sorted(set(opts.settings) & set(OWNED_SETTINGS)):
        bad(f"settings.{key} is set by pipeloom", "remove it from settings")
    tags = opts.settings.get("tags", {})
    if not isinstance(tags, dict):
        bad("settings.tags must be a mapping")
    else:
        for key in sorted(k for k in tags if k.startswith(TAG_PREFIX)):
            bad(f"settings.tags.{key}: the {TAG_PREFIX} prefix is reserved")
    if locs.artifacts_root is None or not _ABSOLUTE.match(locs.artifacts_root):
        bad(
            "the target's artifacts.root is unset or relative",
            "set an absolute path, such as a /Volumes path, that every task can write",
        )
    if not locs.state_shared:
        bad(
            "the target's state store is not shared between machines",
            "use a shared state store, such as the table store",
        )


def _own_version(plan: Plan) -> str:
    try:
        return importlib.metadata.version("pipeloom-databricks")
    except importlib.metadata.PackageNotFoundError:
        return plan.metadata.pipeloom_version


def _pins(plan: Plan, versions: Mapping[str, str] | None) -> dict[str, str]:
    """Distribution -> version the job installs: pipeloom, this renderer and every plugin."""
    pins = {"pipeloom": plan.metadata.pipeloom_version, "pipeloom-databricks": _own_version(plan)}
    for info in plan.metadata.plugins.values():
        if info.distribution and info.version:
            pins.setdefault(info.distribution, info.version)
    pins.update(versions or {})
    return dict(sorted(pins.items()))


def _args(
    plan: Plan, pipeline: str, locs: Locations, run: _Run, command: str, selector: str | None
) -> tuple[str, ...]:
    out = [
        command,
        f"--project-dir={locs.project_root}",
        f"--manifest={locs.manifest_path}",
        f"--target={plan.metadata.target}",
        f"--pipeline={pipeline}",
    ]
    if selector is not None:
        out.append(selector)
    out += [
        f"--plan-hash={plan.metadata.plan_hash}",
        f"--run-id={run.run_id}",
        f"--full-refresh={run.full_refresh}",
        *run.job,
    ]
    if selector is not None:
        out += ["--task-key={{task.name}}", "--task-run-id={{task.run_id}}"]
    out.append(f"--start-ms={run.start_ms}")
    return tuple(out)


def _assemble(
    plan: Plan,
    pipeline: str,
    locs: Locations,
    report: Report,
    versions: Mapping[str, str] | None,
    run: _Run,
    *,
    ephemeral: bool,
) -> _Parts | None:
    opts = deploy_options(plan, report)
    if opts is None:
        return None
    before = report.error_count
    _check_options(opts, locs, report)
    if report.error_count > before:
        return None
    units = build_tasks(plan, opts, pipeline, report)
    if report.error_count > before:
        return None
    used = sorted({u.compute for u in units} | {opts.default_compute})
    serverless = [c for c in used if isinstance(opts.compute[c], ServerlessCompute)]
    if len(serverless) > MAX_ENVIRONMENTS:
        report.add(
            "PLD004",
            f"the job would use {len(serverless)} serverless environments, the limit is "
            f"{MAX_ENVIRONMENTS}",
            hint="use fewer computes in this pipeline",
        )
    if ephemeral and len(serverless) < len(used):
        report.add(
            "PLD004",
            "an ephemeral run cannot use a job cluster",
            hint="use serverless compute, or deploy the job",
        )
    if report.error_count > before:
        return None

    pins = [f"{name}=={version}" for name, version in _pins(plan, versions).items()]
    environments: list[JobEnvironment] = []
    clusters: list[JobCluster] = []
    for name in used:
        compute = opts.compute[name]
        if isinstance(compute, ServerlessCompute):
            deps = tuple(compute.dependencies) if compute.dependencies is not None else tuple(pins)
            spec = EnvironmentSpec(compute.environment_version, deps)
            environments.append(JobEnvironment(f"pipeloom_{name}", spec))
        elif not ephemeral:
            clusters.append(JobCluster(f"pipeloom_{name}", compute.new_cluster))

    def place(
        compute_key: str,
    ) -> tuple[str | None, str | None, tuple[Mapping[str, JsonValue], ...]]:
        compute = opts.compute[compute_key]
        if isinstance(compute, JobClusterCompute):
            libs = compute.libraries
            if libs is None:
                libs = [{"pypi": {"package": p}} for p in pins]
            return None, f"pipeloom_{compute_key}", tuple(libs)
        return f"pipeloom_{compute_key}", None, ()

    tasks: list[Task] = []
    for unit in units:
        env_key, cluster_key, libraries = place(unit.compute)
        configs = [plan.nodes[plan.units[u].nodes[-1]].config for u in unit.units]
        retries = max(c.retries for c in configs)
        timeouts = [c.timeout for c in configs]
        selector = (
            f"--unit={unit.units[0]}" if opts.task_grouping == "none" else f"--group={unit.key}"
        )
        tasks.append(
            Task(
                task_key=unit.key,
                depends_on=tuple(TaskDependency(p) for p in unit.parents),
                run_if=None,
                environment_key=env_key,
                job_cluster_key=cluster_key,
                libraries=libraries,
                python_wheel_task=PythonWheelTask(
                    PACKAGE_NAME, ENTRY_POINT, _args(plan, pipeline, locs, run, "unit", selector)
                ),
                max_retries=retries,
                min_retry_interval_millis=(
                    max(c.retry_delay for c in configs) * 1000 if retries else None
                ),
                timeout_seconds=(
                    sum(t + locs.cancel_grace for t in timeouts if t) + TIMEOUT_MARGIN
                    if all(timeouts)
                    else None
                ),
            )
        )
    waits = {p for u in units for p in u.parents}
    env_key, cluster_key, libraries = place(opts.default_compute)
    tasks.append(
        Task(
            task_key=keys.FINALIZE_KEY,
            depends_on=tuple(TaskDependency(u.key) for u in units if u.key not in waits),
            run_if="ALL_DONE",
            environment_key=env_key,
            job_cluster_key=cluster_key,
            libraries=libraries,
            python_wheel_task=PythonWheelTask(
                PACKAGE_NAME, ENTRY_POINT, _args(plan, pipeline, locs, run, "finalize", None)
            ),
            max_retries=1,
            min_retry_interval_millis=None,
            timeout_seconds=FINALIZE_TIMEOUT,
        )
    )
    return _Parts(opts, units, tuple(tasks), tuple(environments), tuple(clusters))


def render_job(
    plan: Plan,
    pipeline: str,
    locs: Locations,
    report: Report,
    *,
    versions: Mapping[str, str] | None = None,
) -> RenderedJob | None:
    """The job of one pipeline, or None with the problems in `report`.

    `versions` overrides the version pinned for a distribution; a test uses it to keep goldens
    stable across releases.
    """
    parts = _assemble(plan, pipeline, locs, report, versions, _JOB_RUN, ephemeral=False)
    if parts is None:
        return None
    meta = plan.metadata
    job_tag = f"{meta.project}.{pipeline}.{meta.target}"
    user_tags = parts.opts.settings.get("tags", {})
    assert isinstance(user_tags, dict)  # checked by _check_options
    tags = {
        **{k: str(v) for k, v in user_tags.items()},
        "pipeloom_job": job_tag,
        "pipeloom_plan_hash": meta.plan_hash,
        "pipeloom_version": _pins(plan, versions)["pipeloom"],
    }
    settings = JobSettings(
        name=job_name(plan, pipeline),
        tags=dict(sorted(tags.items())),
        parameters=(JobParameter("pipeloom_run_id", ""), JobParameter("full_refresh", "false")),
        max_concurrent_runs=1,
        queue=QueueSettings(),
        environments=parts.environments,
        job_clusters=parts.clusters,
        tasks=parts.tasks,
        extra={k: v for k, v in parts.opts.settings.items() if k != "tags"},
    )
    digest = settings_hash(to_json(settings))
    tags["pipeloom_settings_hash"] = digest
    settings = replace(settings, tags=dict(sorted(tags.items())))
    return RenderedJob(settings, job_tag, digest, parts.units)


def render_submit(
    plan: Plan,
    pipeline: str,
    locs: Locations,
    report: Report,
    *,
    run_id: str,
    full_refresh: bool,
    start_ms: int,
    versions: Mapping[str, str] | None = None,
) -> SubmitSpec | None:
    """The body of a one-time run: serverless only, the run id and start given literally."""
    run = _Run(run_id, "true" if full_refresh else "false", (), str(start_ms))
    parts = _assemble(plan, pipeline, locs, report, versions, run, ephemeral=True)
    if parts is None:
        return None
    return SubmitSpec(job_name(plan, pipeline), parts.tasks, parts.environments, QueueSettings())
