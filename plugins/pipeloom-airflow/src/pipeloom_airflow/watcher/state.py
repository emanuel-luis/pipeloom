"""What a watcher knows from files: where the run writes, what a unit's attempt files and its
results row say, and what the watcher does next (ADR-0048, rule 6). Pure: no Airflow import."""

from __future__ import annotations

import json
import ntpath
import posixpath
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal

import yaml

from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.reference import RESULTS_FILE
from pipeloom.diagnostics import Report
from pipeloom.runtime.retry import Category, UnitPolicy, should_retry
from pipeloom.runtime.run import DEFAULT_ARTIFACTS_ROOT
from pipeloom_airflow.config import _absolute

RECHECK_S: Final = 300.0
"""Seconds a trigger waits before the watcher checks the producer's task state again."""
VISIBILITY_GRACE_S: Final = 120.0
"""Seconds a file may lag behind the producer's outcome (NFS attribute caches) before it counts
as missing."""
TERMINAL_TASK_STATES: Final = frozenset(
    {"success", "failed", "skipped", "upstream_failed", "removed"}
)
"""Producer task states after which it writes nothing more."""


def resolve_artifacts_root(
    project_dir: str,
    exec_project_dir: str,
    target: str,
    override: str | None,
    report: Report,
) -> str | None:
    """The artifacts root the run writes to, as the core resolves it: `targets.<t>.artifacts`
    replaces the top-level block whole, `root` falls back to `target`, and a relative root is
    joined to the execution project dir. Reads `pipeloom.yml` as plain YAML so no user module is
    imported. None and PLA008 when it cannot be resolved without `override`."""
    if override:
        return override
    hint = "set execution.watcher_artifacts_root to the absolute artifacts root"
    try:
        doc = yaml.safe_load((Path(project_dir) / "pipeloom.yml").read_bytes())
    except (OSError, yaml.YAMLError) as err:
        report.add("PLA008", f"cannot read pipeloom.yml under '{project_dir}': {err}", hint=hint)
        return None
    if not isinstance(doc, dict):
        report.add("PLA008", f"pipeloom.yml under '{project_dir}' is not a mapping", hint=hint)
        return None
    targets = doc.get("targets")
    tgt = targets.get(target) if isinstance(targets, dict) else None
    block = tgt.get("artifacts") if isinstance(tgt, dict) else None
    if block is None:
        block = doc.get("artifacts")
    raw = block.get("root") if isinstance(block, dict) else None
    root = DEFAULT_ARTIFACTS_ROOT if raw is None else str(raw)
    if "${" in root:
        report.add("PLA008", f"artifacts.root '{root}' is resolved only at run time", hint=hint)
        return None
    if _absolute(root):
        return root
    if ntpath.isabs(exec_project_dir) and not posixpath.isabs(exec_project_dir):
        return ntpath.join(exec_project_dir, root)
    return posixpath.join(exec_project_dir, root)


@dataclass(frozen=True)
class AttemptView:
    number: int
    status: str
    category: Category | None
    code: str | None
    message: str | None
    row: dict[str, Any]


@dataclass(frozen=True)
class UnitView:
    attempts: tuple[AttemptView, ...]
    """Visible attempt files, by number."""
    result_row: dict[str, Any] | None
    results_present: bool


def _json(path: Path) -> dict[str, Any] | None:
    """A complete JSON object, or None: missing, partial (still being written) or invalid."""
    try:
        doc = json.loads(path.read_bytes())
    except (OSError, ValueError):
        return None
    return doc if isinstance(doc, dict) else None


def _error(row: dict[str, Any]) -> dict[str, Any]:
    error = row.get("error")
    return error if isinstance(error, dict) else {}


def read_unit(run_dir: Path, unit_id: str, *, probe: int) -> UnitView:
    """The unit's attempt files and results row. The folder is listed, and `attempt-1` up to
    `attempt-<probe>` are also opened by name: an NFS directory listing can lag behind a file."""
    paths = RunPaths(run_dir.parent.parent, run_dir.name)
    numbers = set(range(1, probe + 1))
    try:
        numbers.update(paths.attempt_numbers(unit_id))
    except OSError:
        pass
    attempts = []
    for n in sorted(numbers):
        row = _json(paths.attempt(unit_id, n))
        if row is None:
            continue
        error = _error(row)
        category = error.get("category")
        attempts.append(
            AttemptView(
                n,
                str(row.get("status")),
                category if category in ("transient", "permanent") else None,
                error.get("code"),
                error.get("message"),
                row,
            )
        )
    results = _json(run_dir / RESULTS_FILE)
    rows = results.get("results") if results is not None else None
    found = None
    if isinstance(rows, list):
        found = next((r for r in rows if isinstance(r, dict) and r.get("unit_id") == unit_id), None)
    return UnitView(tuple(attempts), found, results is not None)


DecisionKind = Literal["success", "failed", "run", "wait", "invisible", "no_results"]


@dataclass(frozen=True)
class Decision:
    kind: DecisionKind
    attempt: AttemptView | None = None
    code: str | None = None
    message: str | None = None


def _failed(attempt: AttemptView) -> Decision:
    return Decision("failed", attempt, attempt.code, attempt.message)


def decide(view: UnitView, policy: UnitPolicy, *, producer_ended: bool) -> Decision:
    """What the files say. A final attempt wins; then the results row; then the producer's end.
    `invisible` and `no_results` are verdicts only once the visibility grace has passed."""
    latest = view.attempts[-1] if view.attempts else None
    if latest is not None:
        if latest.status == "succeeded":
            return Decision("success", latest)
        if latest.status in ("timed_out", "cancelled"):
            return _failed(latest)
        used = latest.row.get("retries_used")
        used = used if isinstance(used, int) else latest.number - 1
        if latest.status == "failed" and not should_retry(policy, used, latest.category):
            return _failed(latest)
    if view.results_present:
        row = view.result_row or {}
        status = row.get("status")
        if status is None:
            return Decision(
                "invisible",
                code="PLA004",
                message=f"{RESULTS_FILE} has no row for the unit",
            )
        if status == "skipped":
            return Decision("run")
        if status in ("failed", "timed_out", "cancelled") and latest is not None:
            if row.get("attempt") == latest.number:
                # the row confirms the visible attempt was the last one: its status is final
                error = _error(row)
                return Decision(
                    "failed",
                    latest,
                    error.get("code") or latest.code,
                    error.get("message") or latest.message,
                )
        if status == "cancelled":
            error = _error(row)
            return Decision(
                "failed",
                code=error.get("code"),
                message=error.get("message") or "the run was cancelled before the unit ran",
            )
        return Decision(
            "invisible",
            code="PLA004",
            message=f"{RESULTS_FILE} lists the unit as {status}, but no attempt file of that "
            "attempt is visible under artifacts.root (the root must be shared)",
        )
    if producer_ended:
        return Decision(
            "no_results",
            code="PLA009",
            message="the producer task ended, but no attempt file of the unit and no "
            f"{RESULTS_FILE} is visible under artifacts.root",
        )
    return Decision("wait")


def in_progress(view: UnitView, policy: UnitPolicy, *, producer_active: bool) -> bool:
    """The producer may still run the unit: it is active and the files hold no final verdict."""
    final = decide(view, policy, producer_ended=False).kind in ("success", "failed")
    return producer_active and not final
