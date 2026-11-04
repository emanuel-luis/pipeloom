"""`run_results.json` (`pipeloom.run_results/1`): the outcome of one run, one row per model."""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal

RESULTS_SCHEMA: Final = "pipeloom.run_results/1"

RunStatusName = Literal["succeeded", "failed", "cancelled"]

# Every row has every key. Fields 0.2 does not fill stay null or empty.
ROW_DEFAULTS: Final[Mapping[str, Any]] = {
    "unique_id": None,
    "unit_id": None,
    "ephemeral_inlined": [],
    "materialization": None,
    "strategy": None,
    "relation_name": None,
    "status": None,
    "attempt": None,
    "retries_used": 0,
    "started_at": None,
    "ended_at": None,
    "duration_ms": None,
    "queue_wait_ms": None,
    "rows_read": {},
    "rows_written": None,
    "rows_inserted": None,
    "rows_updated": None,
    "rows_deleted": None,
    "rows_dropped": None,
    "rows_quarantined": None,
    "bytes_read": None,
    "bytes_written": None,
    "target_version": None,
    "full_refresh": False,
    "is_incremental": False,
    "watermark_before": None,
    "watermark_after": None,
    "schema_hash": None,
    "schema_changes": [],
    "expectations": [],
    "warnings": [],
    "error": None,
    "engine": None,
    "engine_version": None,
    "executor": None,
    "idempotency_token": None,
    "skip_reason": None,
    "orchestrator_ids": {},
}


def result_row(**fields: Any) -> dict[str, Any]:
    """A results row: the defaults, then `fields`. An unknown field is a bug."""
    unknown = fields.keys() - ROW_DEFAULTS.keys()
    if unknown:
        raise KeyError(f"unknown run_results field(s): {sorted(unknown)}")
    row = {k: (v.copy() if isinstance(v, list | dict) else v) for k, v in ROW_DEFAULTS.items()}
    row.update(fields)
    return row


def schema_hash(columns: Iterable[tuple[str, str, bool]]) -> str:
    """sha256 of the canonical schema, given as (name, dtype, nullable) in column order."""
    doc = [{"name": n, "dtype": d, "nullable": nl} for n, d, nl in columns]
    text = json.dumps(doc, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def git_sha(cwd: Path, timeout: float = 2.0) -> str | None:
    """HEAD of the repository around `cwd`, or None when git is missing, slow or not a repo."""
    try:
        done = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    out = done.stdout.strip()
    return out if done.returncode == 0 and len(out) >= 7 else None


def status_of(rows: Sequence[Mapping[str, Any]]) -> RunStatusName:
    statuses = {r["status"] for r in rows}
    if "cancelled" in statuses:
        return "cancelled"
    if statuses & {"failed", "skipped", "timed_out"}:
        return "failed"
    return "succeeded"


def exit_code_of(status: RunStatusName) -> int:
    return {"succeeded": 0, "failed": 1, "cancelled": 130}[status]


def counts_of(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    out = {"total": len(rows)}
    for state in ("succeeded", "failed", "skipped", "cancelled", "timed_out"):
        out[state] = sum(1 for r in rows if r["status"] == state)
    return out


def totals_of(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    done = [r for r in rows if r["status"] == "succeeded"]
    return {
        "rows_written": sum(r["rows_written"] or 0 for r in done),
        "bytes_written": sum(r["bytes_written"] or 0 for r in done),
    }


@dataclass(frozen=True)
class RunHeader:
    """What the invocation knows; the rows come from the unit attempts."""

    run_id: str
    invocation_id: str
    invocation_ids: Sequence[str]
    """Every invocation that worked on the run, oldest first."""
    project: str
    target: str
    plan_hash: str
    pipeloom_version: str
    plugins: Mapping[str, Any]
    args: Mapping[str, Any]
    selector: Mapping[str, Any]
    full_refresh: bool
    generated_at: str
    started_at: str
    ended_at: str
    duration_ms: int
    identity: Mapping[str, Any]
    python_version: str
    git_sha: str | None


def build_run_results(
    header: RunHeader, rows: Sequence[dict[str, Any]], warnings: Sequence[str] = ()
) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda r: str(r["unit_id"]))
    status = status_of(ordered)
    h = header
    return {
        "schema": RESULTS_SCHEMA,
        "metadata": {
            "pipeloom_version": h.pipeloom_version,
            "generated_at": h.generated_at,
            "invocation_id": h.invocation_id,
            "plugins": dict(h.plugins),
        },
        "args": dict(h.args),
        "run": {
            "run_id": h.run_id,
            "parent_run_id": None,
            "invocation_ids": list(h.invocation_ids),
            "project": h.project,
            "target": h.target,
            "plan_hash": h.plan_hash,
            "selector": dict(h.selector),
            "full_refresh": h.full_refresh,
            "status": status,
            "started_at": h.started_at,
            "ended_at": h.ended_at,
            "duration_ms": h.duration_ms,
            "counts": counts_of(ordered),
            "totals": totals_of(ordered),
            "identity": dict(h.identity),
            "pipeloom_version": h.pipeloom_version,
            "plugins": dict(h.plugins),
            "python_version": h.python_version,
            "git_sha": h.git_sha,
            "exit_code": exit_code_of(status),
            "warnings": list(warnings),
        },
        "results": ordered,
    }
