"""Direct deploy: upload the project, then create or reset the job of one pipeline.

The job's parameters name the upload folder, which is named after the content hash, so a changed
project changes the job settings and a second deploy of the same project is a no-op.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pipeloom.diagnostics import Report
from pipeloom.plan.model import Plan
from pipeloom.secrets.redaction import Redactor
from pipeloom_databricks.bundle import Runtime
from pipeloom_databricks.client import ApiError, DatabricksClient, JobSummary
from pipeloom_databricks.jobs_model import to_json
from pipeloom_databricks.render import Locations, RenderedJob, deploy_options, render_job
from pipeloom_databricks.upload import MANIFEST_PATH, Payload, collect, prune, upload

JOB_TAG = "pipeloom_job"
HASH_TAG = "pipeloom_settings_hash"


class DuplicateJobs(Exception):
    """More than one job carries the same `pipeloom_job` tag (PLD006)."""

    def __init__(self, job_tag: str, job_ids: tuple[int, ...]) -> None:
        super().__init__(f"{len(job_ids)} jobs carry {JOB_TAG}={job_tag}: {list(job_ids)}")
        self.job_tag = job_tag
        self.job_ids = job_ids


@dataclass(frozen=True)
class DeployResult:
    job_id: int
    action: Literal["created", "updated", "unchanged"]
    root: str
    """The workspace folder that holds the uploaded project."""


def find_job(
    client: DatabricksClient, job_tag: str, *, name: str | None = None
) -> JobSummary | None:
    """The job tagged `pipeloom_job=<job_tag>`, or None.

    With `name` the lookup by name goes first (cheap); when it finds no tagged job every job of
    the workspace is scanned, because the tag, not the name, is the identity. Raises
    `DuplicateJobs`.
    """
    for lookup in ([name] if name is not None else []) + [None]:
        hits = [j for j in client.list_jobs(name=lookup) if j.tags.get(JOB_TAG) == job_tag]
        if len(hits) > 1:
            raise DuplicateJobs(job_tag, tuple(sorted(j.job_id for j in hits)))
        if hits:
            return hits[0]
    return None


@dataclass(frozen=True)
class Prepared:
    """Everything a deploy decides before it talks to the workspace."""

    payload: Payload
    parent: str
    """`<workspace_root>/<project>/<pipeline>/<target>`: the folder of all uploads of the job."""
    root: str
    job: RenderedJob
    locations: Locations


def prepare(
    project_dir: Path,
    plan: Plan,
    pipeline: str,
    runtime: Runtime,
    report: Report,
    *,
    user: Callable[[], str],
    versions: Mapping[str, str] | None = None,
) -> Prepared | None:
    """Collect the files and render the job for their upload folder. Touches no workspace except
    through `user`, which is called only when `workspace_root` is not set."""
    opts = deploy_options(plan, report)
    if opts is None:
        return None
    payload = collect(project_dir, plan, runtime.artifacts_root, report)
    if payload is None:
        return None
    meta = plan.metadata
    base = opts.workspace_root or f"/Workspace/Users/{user()}/.pipeloom"
    parent = f"{base.rstrip('/')}/{meta.project}/{pipeline}/{meta.target}"
    root = f"{parent}/{payload.digest[:12]}"
    locs = Locations(
        project_root=root,
        manifest_path=f"{root}/{MANIFEST_PATH}",
        artifacts_root=runtime.artifacts_root,
        state_shared=runtime.state_shared,
        cancel_grace=runtime.cancel_grace,
    )
    job = render_job(plan, pipeline, locs, report, versions=versions)
    return None if job is None else Prepared(payload, parent, root, job, locs)


@contextmanager
def api_errors(report: Report, redactor: Redactor | None = None) -> Iterator[None]:
    """Turn `DuplicateJobs` into PLD006 and `ApiError` into PLD009 in `report`; the block then
    ends early (the caller returns None)."""
    try:
        yield
    except DuplicateJobs as err:
        report.add(
            "PLD006",
            f"{len(err.job_ids)} jobs carry {JOB_TAG}={err.job_tag}: ids {list(err.job_ids)}",
            hint="delete or untag the extra jobs",
        )
    except ApiError as err:
        text = redactor.mask(err.message) if redactor else err.message
        code = f", {err.error_code}" if err.error_code else ""
        report.add(
            "PLD009",
            f"Databricks API request failed (HTTP {err.status}{code}): {text}",
            context={"status": err.status},
        )


def deploy_prepared(
    client: DatabricksClient,
    prepared: Prepared,
    existing: JobSummary | None,
    *,
    report: Report,
    redactor: Redactor | None = None,
) -> DeployResult:
    """Upload and create or reset the job of `prepared`; `existing` is the job found by tag.

    Raises `ApiError`; wrap the call in `api_errors`.
    """
    job, root = prepared.job, prepared.root
    if existing is not None and existing.tags.get(HASH_TAG) == job.settings_hash:
        return DeployResult(existing.job_id, "unchanged", root)
    upload(client, prepared.payload, root)
    settings = to_json(job.settings)
    if existing is None:
        result = DeployResult(client.create_job(settings), "created", root)
    else:
        client.reset_job(existing.job_id, settings)
        result = DeployResult(existing.job_id, "updated", root)
    prune(
        client,
        prepared.parent,
        prepared.payload.digest[:12],
        report,
        mask=redactor.mask if redactor else str,
    )
    return result


def deploy(
    client: DatabricksClient,
    project_dir: Path,
    plan: Plan,
    pipeline: str,
    runtime: Runtime,
    *,
    report: Report,
    redactor: Redactor | None = None,
    versions: Mapping[str, str] | None = None,
) -> DeployResult | None:
    """Deploy one pipeline, or return None with PLD004, PLD006, PLD009 or PLD012 in `report`."""
    with api_errors(report, redactor):
        prepared = prepare(
            project_dir,
            plan,
            pipeline,
            runtime,
            report,
            user=client.current_user,
            versions=versions,
        )
        if prepared is None:
            return None
        existing = find_job(client, prepared.job.job_tag, name=prepared.job.settings.name)
        return deploy_prepared(client, prepared, existing, report=report, redactor=redactor)
    return None
