"""The reference state of `--state DIR`: a manifest, and optionally the run results next to it."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeloom.artifacts.results import RESULTS_SCHEMA
from pipeloom.diagnostics import Location, Report
from pipeloom.graph.selectors import StateRef
from pipeloom.plan.manifest import read_manifest
from pipeloom.plan.model import Plan

MANIFEST_FILE = "manifest.json"
RESULTS_FILE = "run_results.json"


@dataclass(frozen=True)
class Reference:
    """What `--state` points at. `results` maps a unique id to its row status; None without a
    run_results.json."""

    path: Path
    plan: Plan
    results: dict[str, str] | None

    def selector_state(self) -> StateRef:
        return StateRef(
            {
                uid: (n.checksum, tuple(sorted(n.inputs.items())))
                for uid, n in self.plan.nodes.items()
            },
            self.results,
        )


def _shown(path: Path, project_dir: Path | None) -> str:
    if project_dir is not None:
        try:
            return path.resolve().relative_to(project_dir.resolve()).as_posix()
        except ValueError:
            pass
    return path.as_posix()


def _read_results(path: Path, project_name: str) -> tuple[dict[str, str] | None, str | None]:
    """The row statuses by unique id, or None and why the file cannot be used."""
    try:
        data: Any = json.loads(path.read_bytes().decode("utf-8"))
    except OSError as exc:
        return None, type(exc).__name__
    except ValueError as exc:  # JSONDecodeError and UnicodeDecodeError
        return None, f"{type(exc).__name__}: {exc}"
    if not isinstance(data, dict) or data.get("schema") != RESULTS_SCHEMA:
        return None, f"expected schema '{RESULTS_SCHEMA}'"
    run, rows = data.get("run"), data.get("results")
    if not isinstance(run, dict) or not isinstance(rows, list):
        return None, "it has no run or results"
    if run.get("project") != project_name:
        return None, f"it belongs to project '{run.get('project')}', not '{project_name}'"
    try:
        return {r["unique_id"]: r["status"] for r in rows}, None
    except (KeyError, TypeError):
        return None, "a row has no unique_id or status"


def load_reference(
    directory: Path, project_name: str, report: Report, project_dir: Path | None = None
) -> Reference | None:
    """Read `DIR/manifest.json` (required) and `DIR/run_results.json` (optional). Reports PLX110
    and returns None when the folder cannot serve as the reference of this project."""
    shown = _shown(directory, project_dir)

    def fail(message: str, file: str | None = None) -> None:
        report.add(
            "PLX110",
            message,
            location=Location(f"{shown}/{file}" if file else shown),
            hint="compile with --stable into the --state folder, or point --state at target",
        )

    if not directory.is_dir():
        fail(f"state folder '{shown}' does not exist")
        return None
    if not (directory / MANIFEST_FILE).is_file():
        fail(f"state folder '{shown}' has no {MANIFEST_FILE}")
        return None
    found = Report()
    plan = read_manifest(directory / MANIFEST_FILE, found)
    if plan is None:
        why = found.sorted()[0].message.removeprefix("manifest is not usable: ")
        fail(f"reference manifest is not usable: {why}", MANIFEST_FILE)
        return None
    if plan.metadata.project != project_name:
        fail(
            f"reference manifest is for project '{plan.metadata.project}', not '{project_name}'",
            MANIFEST_FILE,
        )
        return None
    results = None
    if (directory / RESULTS_FILE).exists():
        results, problem = _read_results(directory / RESULTS_FILE, project_name)
        if results is None:
            fail(f"reference run results are not usable: {problem}", RESULTS_FILE)
            return None
    return Reference(directory, plan, results)
