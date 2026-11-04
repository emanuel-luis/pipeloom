"""Merge the unit attempts of a run into `run_results.json` and publish the run's files."""

from __future__ import annotations

import json
import shutil
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeloom.artifacts.io import write_json_atomic
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import RunHeader, build_run_results, result_row


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


def finalize(
    paths: RunPaths,
    header: RunHeader,
    *,
    unit_nodes: Mapping[str, str],
    skipped: Mapping[str, tuple[str, str | None]],
    cancelled: Collection[str],
    unfinished: Collection[str] = (),
    crashed: Mapping[str, dict[str, Any]] | None = None,
) -> Finalized:
    """Write `run_results.json` into the run folder and copy it, with the manifest, to the root.

    `unit_nodes` maps unit id to the unique id of its persisted model. A skipped or cancelled
    verdict of this invocation wins over an attempt file; every other unit uses its newest
    attempt, including units of earlier invocations that reused the run id. `unfinished` units
    (an internal error stopped the run) are reported as cancelled. `crashed` rows replace the
    attempt of a unit whose runner failed before it could write one.
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
    rows.update(crashed or {})
    results = build_run_results(header, list(rows.values()))
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
