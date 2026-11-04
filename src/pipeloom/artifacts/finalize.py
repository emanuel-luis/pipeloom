"""Merge the unit attempts of a run into `run_results.json` and publish the run's files."""

from __future__ import annotations

import json
import shutil
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeloom.artifacts.io import write_json_atomic
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import RunHeader, build_run_results, result_row, status_of


@dataclass(frozen=True)
class Finalized:
    run_results: dict[str, Any]
    written: tuple[Path, ...]
    """The run's `run_results.json` and the copies under the artifacts root."""


def latest_attempts(paths: RunPaths) -> dict[str, dict[str, Any]]:
    """The newest attempt file of every unit in the run folder."""
    out: dict[str, dict[str, Any]] = {}
    if not paths.units_dir.is_dir():
        return out
    for folder in sorted(paths.units_dir.iterdir(), key=lambda p: p.name):
        numbers = paths.attempt_numbers(folder.name)
        if numbers:
            raw = paths.attempt(folder.name, numbers[-1]).read_bytes().decode("utf-8")
            out[folder.name] = json.loads(raw)
    return out


def _ended_at(run_dir: Path) -> datetime:
    """When a run ended: `ended_at` of its `run_results.json`, else the folder's mtime."""
    try:
        doc = json.loads((run_dir / "run_results.json").read_bytes().decode("utf-8"))
        ended = datetime.fromisoformat(doc["run"]["ended_at"])
        if ended.tzinfo is not None:
            return ended
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return datetime.fromtimestamp(run_dir.stat().st_mtime, UTC)


def cleanup_intermediates(
    root: Path, *, now: datetime, ttl: float, run_id: str, status: str
) -> list[str]:
    """Delete `runs/<id>/intermediate/` of runs that ended more than `ttl` seconds before `now`.

    The current run keeps its folder, so a failed run can continue, unless `ttl` is 0 and the
    run succeeded. A missing folder is fine. Returns one warning per folder that could not be
    deleted; a deletion error never fails the run.
    """
    runs = root / "runs"
    if not runs.is_dir():
        return []
    warnings: list[str] = []
    for run_dir in sorted(runs.iterdir(), key=lambda p: p.name):
        folder = run_dir / "intermediate"
        if not folder.is_dir():
            continue
        try:
            if run_dir.name == run_id:
                expired = ttl == 0 and status == "succeeded"
            else:
                expired = (now - _ended_at(run_dir)).total_seconds() > ttl
            if expired:
                shutil.rmtree(folder)
        except OSError as exc:
            warnings.append(f"intermediate_cleanup_failed: {run_dir.name}: {exc}")
    return warnings


def finalize(
    paths: RunPaths,
    header: RunHeader,
    *,
    unit_nodes: Mapping[str, str],
    skipped: Mapping[str, tuple[str, str | None]],
    cancelled: Collection[str],
    unfinished: Collection[str] = (),
    crashed: Mapping[str, dict[str, Any]] | None = None,
    timed_out: Mapping[str, tuple[int, dict[str, Any]]] | None = None,
    intermediate_ttl: float | None = None,
) -> Finalized:
    """Write `run_results.json` into the run folder and copy it, with the manifest, to the root.

    `unit_nodes` maps unit id to the unique id of its persisted model. A skipped or cancelled
    verdict of this invocation wins over an attempt file; every other unit uses its newest
    attempt, including units of earlier invocations that reused the run id. A scheduler
    `timed_out` verdict (attempt number and error) also wins over a result written later;
    the late status goes to `warnings` as `late_result: <status>`. `unfinished` units
    (an internal error stopped the run) are reported as cancelled. `crashed` rows replace the
    attempt of a unit whose runner failed before it could write one. With `intermediate_ttl`
    the run also deletes expired intermediates; failures go to the run's `warnings`.
    """
    rows = latest_attempts(paths)
    for unit_id, (reason, _upstream) in skipped.items():
        rows[unit_id] = result_row(
            unique_id=unit_nodes[unit_id], unit_id=unit_id, status="skipped", skip_reason=reason
        )
    for unit_id in (*cancelled, *unfinished):
        rows[unit_id] = result_row(
            unique_id=unit_nodes[unit_id], unit_id=unit_id, status="cancelled"
        )
    for unit_id, (attempt, error) in (timed_out or {}).items():
        late = rows.get(unit_id)
        if late is not None and late["status"] == "timed_out":
            continue  # the owner already recorded it
        warnings = [] if late is None else [f"late_result: {late['status']}"]
        rows[unit_id] = result_row(
            unique_id=unit_nodes[unit_id],
            unit_id=unit_id,
            status="timed_out",
            attempt=attempt,
            error=error,
            warnings=warnings,
        )
    rows.update(crashed or {})
    cleanup_warnings: list[str] = []
    if intermediate_ttl is not None:
        status = status_of(list(rows.values()))
        cleanup_warnings = cleanup_intermediates(
            paths.root,
            now=datetime.fromisoformat(header.ended_at),
            ttl=intermediate_ttl,
            run_id=header.run_id,
            status=status,
        )
    results = build_run_results(header, list(rows.values()), cleanup_warnings)
    write_json_atomic(paths.run_results, results)
    written = [paths.run_results]
    if paths.manifest.is_file():
        for src in (paths.manifest, paths.run_results):
            dest = paths.root / src.name
            tmp = dest.with_name(f"{dest.name}.copy.tmp")
            shutil.copyfile(src, tmp)
            tmp.replace(dest)
            written.append(dest)
    return Finalized(results, tuple(written))
