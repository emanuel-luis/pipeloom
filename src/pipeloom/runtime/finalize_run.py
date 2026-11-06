"""Finalize a run that an orchestrator drove unit by unit: the sequence behind `Project.finalize`.

Every unit of the run wrote its own attempt file. Finalize merges them into `run_results.json`,
fills in the units that wrote none from what the orchestrator knows, records the run, deletes
expired intermediates and emits `run.completed`.
"""

from __future__ import annotations

import hashlib
import json
import platform
from collections.abc import Mapping
from contextlib import ExitStack
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal

import pipeloom
from pipeloom.artifacts.finalize import finalize, latest_attempts
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import (
    RunHeader,
    RunStatusName,
    exit_code_of,
    git_sha,
    result_row,
)
from pipeloom.diagnostics import Diagnostic, JsonValue, Report
from pipeloom.events.bus import EventBus
from pipeloom.events.model import ArtifactWritten, RunCompleted
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import write_manifest
from pipeloom.platform import check_orchestrator_id
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.errors import ErrorInfo, to_diagnostic
from pipeloom.runtime.run import (
    RunOptions,
    RunOutcome,
    _early,
    _iso,
    _missing,
    _plan_from_manifest,
    _rel,
    artifacts_root,
    resolve_run_started_at,
)
from pipeloom.runtime.scheduler import UnitGraph
from pipeloom.secrets.resolve import SecretError, secret_diagnostic
from pipeloom.state.types import LockHeld, RunRecord

if TYPE_CHECKING:
    from pipeloom.api import Project

UNIT_STATES_SCHEMA: Final = "pipeloom.unit_states/1"
_STATUSES: Final = ("skipped", "cancelled", "failed", "excluded")
_DEFAULT_CODE: Final = "PLX106"


@dataclass(frozen=True)
class ExternalUnitState:
    """What the orchestrator knows about a unit that wrote no attempt file."""

    status: Literal["skipped", "cancelled", "failed", "excluded"]
    """`excluded` means the unit was never meant to run (not selected): it gets no row."""
    reason: str | None = None
    """For `skipped`: why; None means `upstream_failed`."""
    code: str | None = None
    message: str | None = None
    orchestrator_ids: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class FinalizeOptions:
    run_id: str
    selection: Selection
    manifest: Path | None = None
    expect_plan_hash: str | None = None
    unit_states: Mapping[str, ExternalUnitState] = field(default_factory=dict)
    orchestrator_ids: Mapping[str, str] = field(default_factory=dict)
    run_started_at: datetime | None = None

    def __post_init__(self) -> None:
        for key, value in self.orchestrator_ids.items():
            check_orchestrator_id(key, value)


def read_unit_states(path: Path) -> dict[str, ExternalUnitState]:
    """Read a `pipeloom.unit_states/1` file. Raises ValueError (or OSError) when it is unusable."""
    return parse_unit_states(path.read_bytes())


def parse_unit_states(data: bytes) -> dict[str, ExternalUnitState]:
    """Parse a `pipeloom.unit_states/1` document. Raises ValueError when it is unusable."""
    try:
        doc = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError(f"not valid JSON: {err}") from err
    if not isinstance(doc, dict) or doc.get("schema") != UNIT_STATES_SCHEMA:
        raise ValueError(f"expected an object with schema '{UNIT_STATES_SCHEMA}'")
    units = doc.get("units")
    if not isinstance(units, dict):
        raise ValueError("'units' must be an object keyed by unit id")  # noqa: TRY004
    out: dict[str, ExternalUnitState] = {}
    for unit_id, raw in units.items():
        if not isinstance(raw, dict):
            raise ValueError(f"unit '{unit_id}': expected an object")  # noqa: TRY004
        status = raw.get("status")
        if status not in _STATUSES:
            raise ValueError(f"unit '{unit_id}': status must be one of {', '.join(_STATUSES)}")
        for name in ("reason", "code", "message"):
            if raw.get(name) is not None and not isinstance(raw[name], str):
                raise ValueError(f"unit '{unit_id}': '{name}' must be a string")
        ids = raw.get("orchestrator_ids", {})
        if not isinstance(ids, dict) or not all(isinstance(v, str) for v in ids.values()):
            raise ValueError(f"unit '{unit_id}': 'orchestrator_ids' must map keys to strings")
        for key, value in ids.items():
            try:
                check_orchestrator_id(key, value)
            except ValueError as err:
                raise ValueError(f"unit '{unit_id}': {err}") from err
        out[unit_id] = ExternalUnitState(
            status=status,
            reason=raw.get("reason"),
            code=raw.get("code"),
            message=raw.get("message"),
            orchestrator_ids=ids,
        )
    return out


def _fails(row: Mapping[str, Any]) -> bool:
    """A row that makes its descendants `upstream_failed`."""
    if row["status"] in ("failed", "timed_out"):
        return True
    return row["status"] == "skipped" and row["skip_reason"] in (
        None,
        "upstream_failed",
        "fail_fast",
    )


def _external_row(
    unit_id: str, node: str, state: ExternalUnitState, run_ids: Mapping[str, str]
) -> dict[str, Any]:
    ids = {**run_ids, **state.orchestrator_ids}
    if state.status == "failed":
        error = ErrorInfo(
            code=state.code or _DEFAULT_CODE,
            category="permanent",
            message=state.message or "the orchestrator reported the unit as failed",
            hint="the unit wrote no attempt file; read the orchestrator's log for the task",
        )
        return result_row(
            unique_id=node,
            unit_id=unit_id,
            status="failed",
            error=error.model_dump(mode="json"),
            orchestrator_ids=ids,
        )
    if state.status == "skipped":
        return result_row(
            unique_id=node,
            unit_id=unit_id,
            status="skipped",
            skip_reason=state.reason or "upstream_failed",
            orchestrator_ids=ids,
        )
    return result_row(unique_id=node, unit_id=unit_id, status="cancelled", orchestrator_ids=ids)


def _refused(bus: EventBus, diags: list[Diagnostic], held: LockHeld) -> RunOutcome:
    info = held.info
    found = Report().add(
        "PLX105",
        f"run {info.run_id} is still running: its lock '{info.scope}' is held by {info.owner} "
        f"until {_iso(info.expires_at)}",
        hint="finalize after the run ended, or after its lease expired",
        context={"owner": info.owner, "run_id": info.run_id, "expires_at": _iso(info.expires_at)},
    )
    return _early(bus, [*diags, found])


def finalize_run(project: Project, opts: FinalizeOptions, bus: EventBus) -> RunOutcome:
    """Merge the unit results of `opts.run_id` into its results and close the run.

    Per unit in scope: its attempt file, else the external state, else a `skipped` row with
    reason `not_run`; an `excluded` state drops the row. The run is failed when any unit failed,
    timed out or was skipped as `upstream_failed`; else cancelled when any was cancelled; else
    succeeded (`not_run` counts for nothing). Repeating the call rewrites the same results.
    """
    run_opts = RunOptions(
        selection=opts.selection, manifest=opts.manifest, expect_plan_hash=opts.expect_plan_hash
    )
    report = Report()
    if opts.manifest is not None:
        plan, bound = _plan_from_manifest(project, run_opts, report)
        diags = list(report.sorted())
    else:
        compiled = project.compile(opts.selection, None)
        plan, bound, diags = compiled.value, project.bind().value, list(compiled.diagnostics)
    if plan is None or bound is None:
        return _early(bus, diags)
    expected = opts.expect_plan_hash
    if expected is not None and plan.metadata.plan_hash != expected.lower():
        found = Report().add(
            "PLX103",
            f"plan hash {plan.metadata.plan_hash[:12]} differs from the expected {expected[:12]}",
            hint="the project or the manifest changed since the hash was taken; compile again",
            context={"expected": expected.lower(), "actual": plan.metadata.plan_hash},
        )
        return _early(bus, [*diags, found])
    engine_plugin, catalog_plugin = bound.plugins.get("engines"), bound.plugins.get("catalogs")
    for plugin, what in ((engine_plugin, "engine"), (catalog_plugin, "catalog")):
        if plugin is None:
            return _early(bus, [*diags, _missing(bound, what)])
    assert engine_plugin is not None and catalog_plugin is not None

    clock = project.clock
    started = clock.now()
    run_id = opts.run_id
    bus.set_run_id(run_id)
    project_dir = bound.loaded.project_dir.resolve()
    paths = RunPaths(artifacts_root(bound, project_dir), run_id)
    chosen = set(plan.selection.selected)
    units = [u for u in plan.units if plan.units[u].nodes[-1] in chosen]
    unit_nodes = {u: plan.units[u].nodes[-1] for u in plan.units}
    identity = project.identity
    ids = {**identity.orchestrator_ids, **opts.orchestrator_ids}
    identity = replace(identity, orchestrator_ids=ids)
    secrets = project.secrets  # resolved here, like `run`, and kept out of every artifact
    try:
        engine = engine_plugin.create(secrets)
        catalog = catalog_plugin.create(secrets)
        state = bound.plugins["state_stores"].create(secrets)
    except SecretError as err:
        return _early(bus, [*diags, secret_diagnostic(err)])
    try:
        with (
            ExitStack() as stack,
            engine.session(
                RuntimeContext(
                    run_id=run_id,
                    invocation_id=project.invocation_id,
                    project=plan.metadata.project,
                    target=plan.metadata.target,
                    project_dir=project_dir,
                    clock=clock,
                    identity=identity,
                    vars=plan.metadata.vars,
                    orchestrator=dict(ids),
                    artifacts_root=paths.root.as_posix(),
                ),
                catalog=catalog,
            ),
        ):
            # Units of an orchestrated run never take the lock. A lock held for this very run id
            # and a run record still `running` mean a `pipeloom run` is working on it:
            # finalizing now would count its in-flight units as not run. Finalize never records
            # `running`, so the lock a crashed finalize left behind does not block its retry.
            # A lock of another run is none of our business.
            prior = state.get_run(run_id)
            try:
                stack.enter_context(
                    state.lock(f"{plan.metadata.project}/{plan.metadata.target}", run_id)
                )
            except LockHeld as held:
                if held.info.run_id == run_id and prior is not None and prior.status == "running":
                    return _refused(bus, diags, held)
            run_started_at, conflict = resolve_run_started_at(
                opts.run_started_at, prior, started, unit_mode=False
            )
            if conflict is not None:
                return _early(bus, [*diags, conflict])
            assert run_started_at is not None
            invocation_ids = tuple(
                dict.fromkeys((*(prior.invocation_ids if prior else ()), project.invocation_id))
            )
            if not paths.manifest.is_file():
                write_manifest(plan, paths.manifest)

            have = latest_attempts(paths)
            external: dict[str, dict[str, Any]] = {}
            for unit_id in units:
                known = opts.unit_states.get(unit_id)
                if unit_id not in have and known is not None and known.status != "excluded":
                    external[unit_id] = _external_row(unit_id, unit_nodes[unit_id], known, ids)
            # A unit the scheduler skipped wrote no attempt file: when nothing else is known,
            # an upstream that failed (walked in topological order) tells why it did not run.
            failing = {u for u, row in {**have, **external}.items() if _fails(row)}
            not_run: dict[str, tuple[str, str | None]] = {}
            for unit_id in UnitGraph.from_plan(plan, plan.units).parents:
                if unit_id not in units or unit_id in have or unit_id in external:
                    continue
                known = opts.unit_states.get(unit_id)
                if known is not None:  # excluded: no row
                    continue
                failed_up = next(
                    (p for p in sorted(plan.units[unit_id].parents) if p in failing), None
                )
                if failed_up is None:
                    not_run[unit_id] = ("not_run", None)
                else:
                    not_run[unit_id] = ("upstream_failed", failed_up)
                    failing.add(unit_id)

            ended = clock.now()
            header = RunHeader(
                run_id=run_id,
                invocation_id=project.invocation_id,
                invocation_ids=invocation_ids,
                project=plan.metadata.project,
                target=plan.metadata.target,
                plan_hash=plan.metadata.plan_hash,
                pipeloom_version=pipeloom.__version__,
                plugins={k: v.model_dump(mode="json") for k, v in plan.metadata.plugins.items()},
                args={
                    "command": "finalize",
                    "select": list(opts.selection.select),
                    "exclude": list(opts.selection.exclude),
                    "manifest": None
                    if opts.manifest is None
                    else _rel(opts.manifest.resolve(), project_dir),
                    "run_id": run_id,
                },
                selector={
                    "select": list(opts.selection.select),
                    "exclude": list(opts.selection.exclude),
                },
                full_refresh=False,
                generated_at=_iso(ended),
                started_at=_iso(run_started_at),
                ended_at=_iso(ended),
                duration_ms=max(0, int((ended - run_started_at).total_seconds() * 1000)),
                identity={
                    "user": identity.user,
                    "host": identity.host,
                    "orchestrator": identity.orchestrator,
                    "orchestrator_ids": dict(ids),
                },
                python_version=platform.python_version(),
                git_sha=git_sha(project_dir),
            )
            done = finalize(
                paths,
                header,
                unit_nodes=unit_nodes,
                skipped=not_run,
                cancelled=(),
                crashed=external,
                intermediate_ttl=bound.runtime.artifacts.intermediate_ttl,
                redactor=project.redactor,
                failure_first=True,
            )
            run = done.run_results["run"]
            status: RunStatusName = run["status"]
            state.record_run(
                RunRecord(
                    run_id=run_id,
                    project=plan.metadata.project,
                    target=plan.metadata.target,
                    plan_hash=plan.metadata.plan_hash,
                    status=status,
                    started_at=run_started_at,
                    ended_at=ended,
                    invocation_ids=invocation_ids,
                    counts=run["counts"],
                )
            )
            written: list[str] = []
            for path in (paths.manifest, *done.written):
                content = path.read_bytes()
                rel = _rel(path, project_dir)
                written.append(rel)
                bus.emit(
                    ArtifactWritten(
                        path=rel,
                        kind="run_results" if path.name == "run_results.json" else "manifest",
                        bytes=len(content),
                        sha256=hashlib.sha256(content).hexdigest(),
                    )
                )
            failures: list[dict[str, JsonValue]] = []
            unit_diags: list[Diagnostic] = []
            for row in done.run_results["results"]:
                if row["error"] is None or row["status"] not in ("failed", "timed_out"):
                    continue
                d = to_diagnostic(ErrorInfo.model_validate(row["error"]), unit_id=row["unit_id"])
                unit_diags.append(d)
                failures.append({"unit_id": row["unit_id"], **d.to_json(), "trace_excerpt": []})
            bus.emit(
                RunCompleted(
                    status=status,
                    exit_code=exit_code_of(status),
                    duration_ms=header.duration_ms,
                    counts=run["counts"],
                    rows_written=run["totals"]["rows_written"],
                    bytes_written=run["totals"]["bytes_written"],
                    failures=failures,
                    artifacts=written,
                    abandoned_threads=0,
                    replay=_is_replay(prior, status, have),
                )
            )
            return RunOutcome(
                status, exit_code_of(status), paths.run_results, (*diags, *unit_diags)
            )
    finally:
        state.close()


def _is_replay(prior: RunRecord | None, status: str, have: Mapping[str, Any]) -> bool:
    """True when the run was already finished with this status and no unit attempt started
    after that: a retried finalize. A repair that ran units again is not a replay."""
    if prior is None or prior.status != status or prior.ended_at is None:
        return False
    starts = [datetime.fromisoformat(r["started_at"]) for r in have.values() if r.get("started_at")]
    return all(s <= prior.ended_at for s in starts)


__all__ = [
    "UNIT_STATES_SCHEMA",
    "ExternalUnitState",
    "FinalizeOptions",
    "finalize_run",
    "parse_unit_states",
    "read_unit_states",
]
