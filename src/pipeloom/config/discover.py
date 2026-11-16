"""Find and read the project file and model files."""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

from pipeloom.config.yaml import LoadedDocument
from pipeloom.diagnostics import Location, Report

PROJECT_FILE = "pipeloom.yml"


def read_file(project_dir: Path, rel: str, report: Report) -> bytes | None:
    """Read a project file as bytes; report PLY004 when it is missing or unreadable."""
    try:
        return (project_dir / rel).read_bytes()
    except OSError as exc:
        reason = "file not found" if isinstance(exc, FileNotFoundError) else exc.strerror
        report.add(
            "PLY004",
            f"cannot read '{rel}': {reason}",
            location=Location(rel),
            hint="run pipeloom from the project directory or pass --project-dir"
            if rel == PROJECT_FILE
            else None,
        )
        return None


def find_model_files(
    project_dir: Path, model_paths: Sequence[str], doc: LoadedDocument, report: Report
) -> tuple[str, ...]:
    """Walk model_paths for properties files (`*.yml`, `*.yaml`), as POSIX paths relative to
    the project directory, sorted; report PLR206 for a missing directory."""
    found: set[str] = set()
    for index, mp in enumerate(model_paths):
        root = project_dir / mp
        if not root.is_dir():
            report.add(
                "PLR206",
                f"model_paths entry '{mp}' does not exist",
                location=doc.locate(f"/model_paths/{index}"),
                hint="create the directory or remove the entry",
            )
            continue
        for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
            dirnames[:] = sorted(d for d in dirnames if not d.startswith("."))
            for name in filenames:
                if name.endswith((".yml", ".yaml")):
                    rel = Path(dirpath, name).relative_to(project_dir).as_posix()
                    if rel != PROJECT_FILE:
                        found.add(rel)
    return tuple(sorted(found))
