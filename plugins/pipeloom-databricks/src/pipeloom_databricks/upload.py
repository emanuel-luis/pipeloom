"""The project files of a direct deploy: what to upload, where, and how old copies are pruned.

The upload set is content addressed. Its folder is named after a hash of the files, a marker file
is written last, and a folder that already holds the marker is never uploaded again, so two
deploys of the same project share one copy and a half-finished upload is simply redone.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pipeloom.diagnostics import Location, Report, Severity
from pipeloom.plan.manifest import manifest_bytes
from pipeloom.plan.model import Plan
from pipeloom_databricks.client import ApiError, DatabricksClient

MAX_FILE_BYTES: Final = 10 * 1024 * 1024
"""The workspace import limit."""
MANIFEST_PATH: Final = "_pipeloom/manifest.json"
MARKER_PATH: Final = "_pipeloom/complete"
KEEP_FOLDERS: Final = 3
SKIPPED_FOLDERS: Final = frozenset(
    {"__pycache__", "venv", ".venv", "dist", "build", "node_modules"}
)
CONFIG_FILE: Final = "pipeloom.yml"
_HASH_DIR = re.compile(r"^[0-9a-f]{12}$")
_NOTEBOOK_HEADERS: Final = (
    b"# Databricks notebook source",
    b"-- Databricks notebook source",
    b"// Databricks notebook source",
)


@dataclass(frozen=True)
class Payload:
    files: tuple[tuple[str, bytes], ...]
    """(POSIX path relative to the project, bytes), sorted by path; the manifest included."""
    digest: str
    """sha256 over every path and its bytes."""


def _digest(files: tuple[tuple[str, bytes], ...]) -> str:
    h = hashlib.sha256()
    for path, data in files:
        h.update(path.encode("utf-8") + b"\0" + str(len(data)).encode("ascii") + b"\0" + data)
    return h.hexdigest()


def collect(
    project_dir: Path, plan: Plan, artifacts_root: str | None, report: Report
) -> Payload | None:
    """The files to upload, or None with PLD012 for each file that cannot be a plain file.

    The set is `pipeloom.yml`, the model and source files of the plan, the `*.py` files below
    `python_paths` and the stable manifest. Below `python_paths` the walk skips dot entries, the
    folders in `SKIPPED_FOLDERS`, any folder holding `pyvenv.cfg` (a virtual environment) and the
    artifacts root, so data files, wheels and credentials of the project folder stay local.
    """
    before = report.error_count
    root = project_dir.resolve()
    skip = Path(artifacts_root).resolve() if artifacts_root else None
    found: dict[str, Path] = {}

    def add(path: Path) -> None:
        try:
            rel = path.resolve().relative_to(root).as_posix()
        except ValueError:
            report.add("PLD012", f"{path.as_posix()} lies outside the project folder")
            return
        found.setdefault(rel, path)

    add(root / CONFIG_FILE)
    for rel_file in [n.file for n in plan.nodes.values()] + [s.file for s in plan.sources.values()]:
        add(root / rel_file)
    for entry in plan.metadata.python_paths:
        base = (root / entry).resolve()
        if not base.is_dir():
            continue
        try:
            base.relative_to(root)
        except ValueError:
            report.add("PLD012", f"python_paths entry '{entry}' lies outside the project folder")
            continue
        for folder, dirs, names in os.walk(base):
            dirs[:] = sorted(
                d
                for d in dirs
                if not d.startswith(".")
                and d not in SKIPPED_FOLDERS
                and not (Path(folder) / d / "pyvenv.cfg").exists()
                and (skip is None or (Path(folder) / d).resolve() != skip)
            )
            for name in sorted(names):
                if name.endswith(".py") and not name.startswith("."):
                    add(Path(folder) / name)

    files: list[tuple[str, bytes]] = []
    for rel in sorted(found):
        data = _read(found[rel], rel, report)
        if data is not None:
            files.append((rel, data))
    if report.error_count > before:
        return None
    files.append((MANIFEST_PATH, manifest_bytes(plan, stable=True)))
    ordered = tuple(sorted(files))
    return Payload(ordered, _digest(ordered))


def _read(path: Path, rel: str, report: Report) -> bytes | None:
    def bad(message: str, hint: str | None = None) -> None:
        report.add("PLD012", message, hint=hint, location=Location(file=rel))

    try:
        if path.stat().st_size > MAX_FILE_BYTES:
            bad("the file is above 10 MB", "keep large files out of the project folders")
            return None
        data = path.read_bytes()
    except OSError as err:
        bad(f"the file cannot be read: {err.strerror or type(err).__name__}")
        return None
    if rel.endswith(".ipynb") or data.lstrip().startswith(_NOTEBOOK_HEADERS):
        bad(
            "the file is a notebook, and the workspace would import it as one",
            "remove it from the project or move it out of python_paths",
        )
        return None
    return data


def is_complete(client: DatabricksClient, root: str) -> bool:
    """Whether `root` holds the marker of a finished upload."""
    try:
        entries = client.list_dir(f"{root}/{MARKER_PATH.rsplit('/', 1)[0]}")
    except ApiError as err:
        if err.status == 404 or err.error_code == "RESOURCE_DOES_NOT_EXIST":
            return False
        raise
    marker = MARKER_PATH.rsplit("/", 1)[1]
    return any(e.path.rsplit("/", 1)[-1] == marker and not e.is_dir for e in entries)


def upload(client: DatabricksClient, payload: Payload, root: str) -> bool:
    """Store the payload under `root`; False when a finished copy is already there.

    The marker is written last, and written again when the copy exists: its modification time
    is the deploy time that `prune` orders folders by (the API gives folders no time).
    """
    marker = payload.digest.encode("ascii") + b"\n"
    if is_complete(client, root):
        client.upload(f"{root}/{MARKER_PATH}", marker)
        return False
    folders = sorted(
        {f"{root}/{p.rsplit('/', 1)[0]}" if "/" in p else root for p, _ in payload.files}
    )
    for folder in folders:
        client.mkdirs(folder)
    for path, data in payload.files:
        client.upload(f"{root}/{path}", data)
    client.upload(f"{root}/{MARKER_PATH}", marker)
    return True


def _deployed_at(client: DatabricksClient, folder: str) -> int | None:
    """When `folder` was last deployed: the modification time of its marker file, 0 when it has
    none (an unfinished upload), None when it cannot be listed."""
    parent, marker = MARKER_PATH.rsplit("/", 1)
    try:
        entries = client.list_dir(f"{folder}/{parent}")
    except ApiError as err:
        return 0 if err.status == 404 or err.error_code == "RESOURCE_DOES_NOT_EXIST" else None
    return max((e.modified_ms for e in entries if e.path.rsplit("/", 1)[-1] == marker), default=0)


def prune(
    client: DatabricksClient,
    parent: str,
    keep_name: str,
    report: Report,
    *,
    mask: Callable[[str], str] = str,
) -> None:
    """Delete the oldest hash folders of `parent`, keeping `keep_name` and the newest others so
    that `KEEP_FOLDERS` remain. Failures are warnings."""

    def warn(what: str, err: ApiError) -> None:
        report.add(
            "PLD009",
            f"{what}: {mask(err.message)}",
            severity=Severity.WARNING,
            context={"status": err.status},
        )

    try:
        entries = client.list_dir(parent)
    except ApiError as err:
        warn(f"cannot list {parent} to prune old uploads", err)
        return
    stamps: dict[str, int] = {}
    for entry in entries:
        name = entry.path.rsplit("/", 1)[-1]
        if entry.is_dir and _HASH_DIR.match(name) and name != keep_name:
            stamp = _deployed_at(client, f"{parent}/{name}")
            if stamp is not None:  # a folder that cannot be inspected is never deleted
                stamps[name] = stamp
    # The newest others include the folder the job used before this deploy: it was deployed last.
    old = sorted(stamps, key=lambda n: (-stamps[n], n))
    for name in old[KEEP_FOLDERS - 1 :]:
        try:
            client.delete(f"{parent}/{name}", recursive=True)
        except ApiError as err:
            warn(f"cannot delete the old upload {parent}/{name}", err)
