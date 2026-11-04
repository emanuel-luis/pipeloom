"""Merge the unit attempts of a run into `run_results.json` and publish the run's files."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeloom.artifacts.io import write_json_atomic
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import (
    RunHeader,
    RunStatusName,
    build_run_results,
    result_row,
    status_of,
)
from pipeloom.secrets.redaction import Redactor


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


_HOUR = 3600.0


def _newest_mtime(folder: Path) -> datetime:
    """The newest modification time of a folder and everything under it."""
    newest = folder.stat().st_mtime
    for dirpath, dirnames, filenames in os.walk(folder):
        for name in (*dirnames, *filenames):
            try:
                newest = max(newest, os.stat(os.path.join(dirpath, name)).st_mtime)
            except OSError:
                continue  # removed while we looked
    return datetime.fromtimestamp(newest, UTC)


def _recorded(run_dir: Path) -> tuple[datetime | None, float | None]:
    """`ended_at` and `intermediate_ttl` of the run's `run_results.json`, None where absent."""
    try:
        run = json.loads((run_dir / "run_results.json").read_bytes().decode("utf-8"))["run"]
        parsed = datetime.fromisoformat(run["ended_at"])
        ttl = run.get("intermediate_ttl")
        return (
            parsed if parsed.tzinfo is not None else None,
            float(ttl) if isinstance(ttl, int | float) else None,
        )
    except (OSError, ValueError, KeyError, TypeError):
        return None, None


def _resumed(run_dir: Path) -> bool:
    """The manifest is newer than the results: a later invocation of the run is in progress."""
    try:
        return (run_dir / "manifest.json").stat().st_mtime > (
            run_dir / "run_results.json"
        ).stat().st_mtime
    except OSError:
        return False


def cleanup_intermediates(
    root: Path, *, now: datetime, ttl: float, run_id: str, status: RunStatusName
) -> list[str]:
    """Delete the `runs/<id>/intermediate/` folders that nobody needs any more.

    The current run's folder goes only when `ttl` is 0 and the run succeeded. Another run is
    judged by the TTL it recorded in its own `run_results.json` (a TTL of 0 there counts as one
    hour, since only a failed run is left with data; `ttl` stands in when none is recorded),
    against its last activity: the newest of its `ended_at` and the newest modification time
    under its run folder (the manifest is rewritten at each start, events grow during a run).
    A run whose manifest is newer than its results is running again and is never deleted. A run without `run_results.json` is still running or
    crashed, so it keeps its folder until it has been idle for `max(ttl, 1h)`. A missing folder
    is fine. Returns one warning per folder that could not be deleted; a deletion error never
    fails the run.
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
                ended, own = _recorded(run_dir)
                if _resumed(run_dir):
                    continue  # started again after its last results: it is running
                active = _newest_mtime(run_dir)  # manifest, events and attempts count too
                if ended is None:
                    limit = max(ttl, _HOUR)
                else:
                    active = max(active, ended)
                    limit = ttl if own is None else own
                    limit = limit if limit > 0 else _HOUR
                expired = (now - active).total_seconds() > limit
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
    redactor: Redactor | None = None,
) -> Finalized:
    """Write `run_results.json` into the run folder and copy it, with the manifest, to the root.

    `unit_nodes` maps unit id to the unique id of its persisted model. A skipped or cancelled
    verdict of this invocation wins over an attempt file; every other unit uses its newest
    attempt, including units of earlier invocations that reused the run id. A scheduler
    `timed_out` verdict (attempt number and error) also wins over a result written later;
    the late status goes to `warnings` as `late_result: <status>`. `unfinished` units
    (an internal error stopped the run) are reported as cancelled. `crashed` rows replace the
    attempt of a unit whose runner failed before it could write one. With `intermediate_ttl`
    the run also deletes expired intermediates; failures go to the run's `warnings`. `redactor`
    masks resolved secrets in everything written.
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
    results = build_run_results(header, list(rows.values()), cleanup_warnings, intermediate_ttl)
    if redactor is not None:
        results = redactor.scrub(results)
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
