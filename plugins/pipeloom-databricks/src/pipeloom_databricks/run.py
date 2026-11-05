"""Start a run of a deployed job (or a one-time run), find a run by its pipeloom id, and wait.

Nothing here builds a client: the caller hands one in, so every function runs on `FakeDatabricks`
in tests. API failures become PLD009 in the `Report`; a function that cannot go on returns None.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Final

from pipeloom.diagnostics import Report, Severity
from pipeloom.graph.selectors import Selection, SelectorError, apply_to_plan
from pipeloom.plan.model import Plan
from pipeloom.secrets.redaction import Redactor
from pipeloom_databricks.bundle import Runtime
from pipeloom_databricks.client import ApiError, DatabricksClient, RunInfo
from pipeloom_databricks.deploy import (
    HASH_TAG,
    JOB_TAG,
    Prepared,
    api_errors,
    deploy_prepared,
    find_job,
    prepare,
)
from pipeloom_databricks.jobs_model import SubmitSpec, TaskDependency, to_json
from pipeloom_databricks.keys import FINALIZE_KEY
from pipeloom_databricks.render import render_submit
from pipeloom_databricks.upload import prune, upload

TERMINAL: Final = frozenset({"TERMINATED", "SKIPPED", "INTERNAL_ERROR"})
"""Life cycle states after which a run does not change."""
POLL_SECONDS: Final = 15
_DBX_RUN = re.compile(r"^dbx-(\d+)$")


class SelectionError(Exception):
    """`--select` and `--exclude` are unusable or match no unit of the pipeline (exit 2)."""


class WaitTimeout(Exception):
    """The run did not end within the time the caller allowed (exit 1)."""


@dataclass(frozen=True)
class RunStarted:
    job_id: int | None
    """None for a one-time run."""
    job_run_id: int
    run_id: str | None
    """The pipeloom run id given or generated; None when the job names it `dbx-<job_run_id>`."""
    run_page_url: str | None


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _page_url(client: DatabricksClient, job_run_id: int) -> str | None:
    try:
        return client.get_run(job_run_id).run_page_url
    except ApiError:
        return None  # the link is a courtesy: never fail a started run over it


def task_keys_for(
    prepared: Prepared, plan: Plan, selection: Selection | None, report: Report
) -> list[str] | None:
    """The task keys the selection reaches, or None for no selection.

    Raises `SelectionError` for a bad selector or when no unit of the pipeline is selected. A
    group that is only partly selected runs whole, which is a warning.
    """
    if selection is None or not (selection.select or selection.exclude):
        return None
    try:
        chosen = apply_to_plan(selection, plan, Report())
    except SelectorError as err:
        raise SelectionError(str(err)) from err
    keys: list[str] = []
    for task in prepared.job.tasks:
        picked = [u for u in task.units if plan.units[u].nodes[-1] in chosen]
        if not picked:
            continue
        keys.append(task.key)
        if len(picked) < len(task.units):
            report.add(
                "PLD015",
                f"the selection covers part of task group '{task.key}'; the whole group runs",
                severity=Severity.WARNING,
                context={"group": task.key},
            )
    if not keys:
        raise SelectionError("the selection matches no unit of the pipeline")
    return keys


def run_job(
    client: DatabricksClient,
    project_dir: Path,
    plan: Plan,
    pipeline: str,
    runtime: Runtime,
    *,
    selection: Selection | None,
    run_id: str | None,
    full_refresh: bool,
    ensure: bool,
    report: Report,
    redactor: Redactor | None = None,
    versions: Mapping[str, str] | None = None,
) -> RunStarted | None:
    """Run the deployed job of `pipeline` now.

    PLD007 when no job is deployed and PLD008 when the deployed job differs from the local
    render, unless `ensure` deploys first. With `selection`, only the tasks it reaches (and
    finalize) run. `run_id` names the run and makes the start idempotent. Raises `SelectionError`.
    """
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
        job = prepared.job
        keys = task_keys_for(prepared, plan, selection, report)
        existing = find_job(client, job.job_tag, name=job.settings.name)
        stale = existing is None or existing.tags.get(HASH_TAG) != job.settings_hash
        if stale and not ensure:
            if existing is None:
                report.add(
                    "PLD007",
                    f"no job carries {JOB_TAG}={job.job_tag}",
                    hint="deploy it with 'pipeloom databricks deploy', or pass --ensure",
                )
            else:
                report.add(
                    "PLD008",
                    f"job {existing.job_id} was deployed from other settings than the project "
                    "renders now",
                    hint="deploy it again with 'pipeloom databricks deploy', or pass --ensure",
                    context={"job_id": existing.job_id},
                )
            return None
        if stale:
            job_id = deploy_prepared(
                client, prepared, existing, report=report, redactor=redactor
            ).job_id
        else:
            assert existing is not None
            job_id = existing.job_id
        params = {"full_refresh": "true" if full_refresh else "false"}
        token = None
        if run_id is not None:
            params["pipeloom_run_id"] = run_id
            token = _digest(f"{job_id}:{run_id}")
        only = None if keys is None else [*keys, FINALIZE_KEY]
        job_run_id = client.run_now(
            job_id, job_parameters=params, only=only, idempotency_token=token
        )
        return RunStarted(job_id, job_run_id, run_id, _page_url(client, job_run_id))
    return None


def _restrict(spec: SubmitSpec, keys: list[str]) -> SubmitSpec:
    """Keep the selected tasks and finalize; finalize then waits for the selection's leaves."""
    inner = {t.task_key for t in spec.tasks if t.task_key in keys or t.task_key == FINALIZE_KEY}
    waited = {
        d.task_key
        for t in spec.tasks
        if t.task_key in inner and t.task_key != FINALIZE_KEY
        for d in t.depends_on
    }
    tasks = []
    for t in spec.tasks:
        if t.task_key not in inner:
            continue
        if t.task_key == FINALIZE_KEY:
            deps = tuple(TaskDependency(k) for k in sorted(inner - waited - {FINALIZE_KEY}))
        else:
            deps = tuple(d for d in t.depends_on if d.task_key in inner)
        tasks.append(replace(t, depends_on=deps))
    return replace(spec, tasks=tuple(tasks))


def submit_ephemeral(
    client: DatabricksClient,
    project_dir: Path,
    plan: Plan,
    pipeline: str,
    runtime: Runtime,
    *,
    selection: Selection | None,
    full_refresh: bool,
    report: Report,
    redactor: Redactor | None = None,
    versions: Mapping[str, str] | None = None,
    run_id: str | None = None,
    start_ms: int | None = None,
) -> RunStarted | None:
    """Run `pipeline` once without a job: upload the project as deploy does, then runs/submit.

    The run id is `run_id` or a generated one, and the start is idempotent on it. Raises
    `SelectionError`.
    """
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
        keys = task_keys_for(prepared, plan, selection, report)
        local_id = run_id or f"eph-{uuid.uuid4().hex[:12]}"
        spec = render_submit(
            plan,
            pipeline,
            prepared.locations,
            report,
            run_id=local_id,
            full_refresh=full_refresh,
            start_ms=int(time.time() * 1000) if start_ms is None else start_ms,
            versions=versions,
        )
        if spec is None:
            return None
        if keys is not None:
            spec = _restrict(spec, keys)
        upload(client, prepared.payload, prepared.root)
        prune(
            client,
            prepared.parent,
            prepared.payload.digest[:12],
            report,
            mask=redactor.mask if redactor else str,
        )
        job_run_id = client.submit_run(to_json(spec), idempotency_token=_digest(local_id))
        return RunStarted(None, job_run_id, local_id, _page_url(client, job_run_id))
    return None


def find_run(client: DatabricksClient, job_id: int, run_id: str) -> RunInfo | None:
    """The run of job `job_id` that has pipeloom run id `run_id`, or None.

    `dbx-<n>` is job run `n`; any other id is looked up in the job's runs by the
    `pipeloom_run_id` job parameter (newest first, as the API lists them).
    """
    named = _DBX_RUN.match(run_id)
    if named:
        try:
            run = client.get_run(int(named.group(1)))
        except ApiError as err:
            if err.status == 404 or err.error_code == "RESOURCE_DOES_NOT_EXIST":
                return None
            raise
        return run if run.job_id == job_id else None
    for run in client.list_runs(job_id):
        if run.job_parameters.get("pipeloom_run_id") == run_id:
            return run
    return None


def effective_run_id(run: RunInfo) -> str:
    """The pipeloom run id of a job run: its parameter, else `dbx-<job run id>`."""
    return run.job_parameters.get("pipeloom_run_id") or f"dbx-{run.run_id}"


def _line(life: str, result: str | None) -> str:
    return f"{life}/{result}" if result else life


def wait_for_run(
    client: DatabricksClient,
    job_run_id: int,
    *,
    sleep: Callable[[float], None],
    say: Callable[[str], None],
    poll: float = POLL_SECONDS,
    repair_id: int | None = None,
    timeout: float | None = None,
) -> RunInfo:
    """Poll until the run ends, saying every task transition. Raises `ApiError`, and
    `WaitTimeout` once `timeout` seconds of polling have passed.

    After a repair, `repair_id` makes the wait ignore the terminal state the run still shows
    right after the repair: it ends only when the run reports that repair or a later one
    (`latest_repair_id >=`) and, for our own repair, when its repair history entry is terminal or
    the run was seen active at least once. A newer repair by someone else ends the wait, since
    ours is over. `timeout` is the backstop for a history entry without a usable state.
    """
    seen: dict[str, str] = {}
    active = False
    waited = 0.0
    while True:
        run = client.get_run(job_run_id)
        now = {"run": _line(run.life_cycle_state, run.result_state)}
        now.update({t.task_key: _line(t.life_cycle_state, t.result_state) for t in run.tasks})
        for key, state in now.items():
            if seen.get(key) != state:
                say(f"{key}: {state}")
        seen = now
        terminal = run.life_cycle_state in TERMINAL
        if not terminal:
            active = True
        if terminal and repair_id is None:
            return run
        if terminal and repair_id is not None:
            latest = run.latest_repair_id or 0
            ours_done = run.repair_states.get(repair_id) in TERMINAL
            if latest > repair_id or (latest == repair_id and (ours_done or active)):
                return run
        if timeout is not None and waited >= timeout:
            raise WaitTimeout(f"run {job_run_id} did not end within {timeout:g} seconds")
        sleep(poll)
        waited += poll


def failure_lines(client: DatabricksClient, artifacts_root: str | None, run_id: str) -> list[str]:
    """One line per failed unit of the run, read from run_results.json on the artifacts volume.

    A line that says why the file could not be read stands in when it is missing or unusable.
    """
    if not artifacts_root:
        return []
    path = f"{artifacts_root.rstrip('/')}/runs/{run_id}/run_results.json"
    try:
        doc = json.loads(client.read_file(path).decode("utf-8"))
        rows = doc["results"]
    except ApiError as err:
        return [f"run_results.json not read ({path}): {err.message}"]
    except (ValueError, KeyError, TypeError):
        return [f"run_results.json is not usable ({path})"]
    lines = []
    for row in rows:
        if row.get("status") in ("failed", "timed_out"):
            error = row.get("error") or {}
            lines.append(f"{row['unit_id']}: {error.get('code', '')} {error.get('message', '')}")
    return [line.rstrip() for line in lines]
