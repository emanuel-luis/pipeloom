"""Load the plan a DAG renders: from a stable manifest, or by compiling the project (cached).

Airflow 3 parses the DAG file before every task, so a compile result is kept on disk, keyed by
everything that can change it. Compiling runs no user code (ADR-0003).
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import posixpath
import tempfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal

import yaml

from pipeloom.diagnostics import Location, Report
from pipeloom.plan.manifest import canonical_json, manifest_bytes, read_manifest
from pipeloom.plan.model import Plan
from pipeloom_airflow.config import LoadMode, ProjectConfig, RenderConfig

CACHE_SCHEMA: Final = "pipeloom_airflow.cache/1"
KEEP: Final = 8
DEFAULT_ARTIFACTS_ROOT: Final = "target"
# ponytail: the root a pipeloom.yml names is read as plain YAML; an interpolated root is not
# skipped, which only makes the key more sensitive.


@dataclass(frozen=True)
class LoadedPlan:
    plan: Plan
    plan_hash: str
    manifest_path: str | None
    source: Literal["manifest", "project", "cache"]


def default_cache_dir() -> str:
    return os.path.join(tempfile.gettempdir(), "pipeloom-airflow-cache")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _versions() -> dict[str, str]:
    found: dict[str, str] = {}
    for dist in importlib.metadata.distributions():
        name = dist.metadata["Name"]
        if name and name.lower().replace("_", "-").startswith("pipeloom"):
            found[name.lower().replace("_", "-")] = dist.version
    return dict(sorted(found.items()))


def _artifact_roots(doc: Mapping[str, Any]) -> set[str]:
    roots = {DEFAULT_ARTIFACTS_ROOT}
    blocks = [doc, *(t for t in (doc.get("targets") or {}).values() if isinstance(t, dict))]
    for block in blocks:
        art = block.get("artifacts")
        if isinstance(art, dict) and isinstance(art.get("root"), str):
            roots.add(art["root"])
    return {posixpath.normpath(r) for r in roots if not posixpath.isabs(r) and ":" not in r}


def _walk(project_dir: Path, rel: str, skip: set[str]) -> Iterator[tuple[str, int, int]]:
    start = project_dir / rel
    if start.is_file():
        found: list[Path] = [start]
    else:
        found = []
        for base, dirs, names in os.walk(start):
            here = Path(base).relative_to(project_dir).as_posix()
            dirs[:] = [
                d
                for d in dirs
                if not d.startswith(".")
                and d != "__pycache__"
                and posixpath.normpath(posixpath.join(here, d)) not in skip
            ]
            found += [Path(base) / n for n in names if not n.startswith(".")]
    for path in found:
        st = path.stat()
        yield path.relative_to(project_dir).as_posix(), st.st_size, st.st_mtime_ns


def _cache_key(project_dir: Path, target: str, vars_: Mapping[str, Any]) -> str:
    """sha256 of the inputs of a compile. Raises OSError or ValueError when they cannot be read."""
    doc = yaml.safe_load((project_dir / "pipeloom.yml").read_bytes())
    if not isinstance(doc, dict):
        raise TypeError("pipeloom.yml is not a mapping")
    skip = _artifact_roots(doc)
    roots = [*(doc.get("model_paths") or ["models"]), *(doc.get("python_paths") or ["."])]
    files: set[tuple[str, int, int]] = set()
    for rel in roots:
        files.update(_walk(project_dir, posixpath.normpath(str(rel)), skip))
    files.update(_walk(project_dir, "pipeloom.yml", skip))
    body = {
        "schema": CACHE_SCHEMA,
        "versions": _versions(),
        "target": target,
        "vars": dict(vars_),
        "files": [list(f) for f in sorted(files)],
    }
    return hashlib.sha256(canonical_json(body)).hexdigest()


def _fingerprint(plan: Plan, env: Mapping[str, str]) -> dict[str, str]:
    return {
        v.name: _sha("set:" + env[v.name]) if v.name in env else _sha("unset")
        for v in plan.metadata.env_vars
    }


def _atomic(path: Path, data: bytes) -> None:
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def _prune(folder: Path) -> None:
    entries = sorted(folder.glob("*.entry.json"), key=lambda p: (p.stat().st_mtime_ns, p.name))
    for entry in entries[:-KEEP]:
        stem = entry.name.removesuffix(".entry.json")
        entry.unlink(missing_ok=True)
        (folder / f"{stem}.manifest.json").unlink(missing_ok=True)


def _from_cache(folder: Path, key: str, env: Mapping[str, str]) -> Plan | None:
    try:
        meta = json.loads((folder / f"{key}.entry.json").read_bytes().decode("utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(meta, dict) or meta.get("schema") != CACHE_SCHEMA or meta.get("key") != key:
        return None
    path = folder / f"{key}.manifest.json"
    try:
        before = hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None
    # Two processes with different env values share these paths, so the entry names the exact
    # manifest it was written with; a manifest from another writer is a miss.
    if meta.get("manifest_sha256") != before:
        return None
    plan = read_manifest(path, Report())
    if plan is None or meta.get("env") != _fingerprint(plan, env):
        return None
    try:
        if hashlib.sha256(path.read_bytes()).hexdigest() != before:
            return None
    except OSError:
        return None
    return plan


def _store(folder: Path, key: str, plan: Plan, env: Mapping[str, str]) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    content = manifest_bytes(plan, stable=True)
    _atomic(folder / f"{key}.manifest.json", content)
    meta = {
        "schema": CACHE_SCHEMA,
        "key": key,
        "env": _fingerprint(plan, env),
        "manifest_sha256": hashlib.sha256(content).hexdigest(),
    }
    _atomic(folder / f"{key}.entry.json", canonical_json(meta) + b"\n")
    _prune(folder)


def _load_manifest(project: ProjectConfig, target: str, report: Report) -> LoadedPlan | None:
    path = Path(project.manifest_path or "")
    plan = read_manifest(path, report)
    if plan is None:
        return None
    if plan.metadata.target != target:
        report.add(
            "PLX107",
            f"manifest is not usable: it was compiled for target '{plan.metadata.target}', "
            f"not '{target}'",
            location=Location(path.as_posix()),
            hint="compile the manifest for this target",
        )
        return None
    return LoadedPlan(plan, plan.metadata.plan_hash, project.manifest_path, "manifest")


def load_plan(
    project: ProjectConfig,
    render: RenderConfig,
    target: str,
    report: Report,
    *,
    env: Mapping[str, str],
) -> LoadedPlan | None:
    """The plan for `target`, or None after reporting why. `env` is `os.environ` itself."""
    if render.load_method is LoadMode.MANIFEST:
        return _load_manifest(project, target, report)
    project_dir = Path(project.project_dir)
    folder: Path | None = None
    key = ""
    try:
        key = _cache_key(project_dir, target, project.vars)
        base = Path(project.cache_dir or default_cache_dir())
        folder = base / _sha(project_dir.as_posix())[:12]
        hit = _from_cache(folder, key, env)
        if hit is not None:
            return LoadedPlan(hit, hit.metadata.plan_hash, None, "cache")
    except (OSError, TypeError, yaml.YAMLError):
        folder = None
        _unusable(report)
    # imported here: a manifest load or a cache hit (every parse but the first) never needs it
    from pipeloom.api import Project, ProjectOptions

    outcome = Project(
        ProjectOptions(project_dir=project_dir, target=target, vars=dict(project.vars), env=env)
    ).compile()
    report.extend(outcome.diagnostics)
    plan = outcome.value
    if plan is None:
        return None
    if folder is not None:
        try:
            _store(folder, key, plan, env)
        except OSError:
            _unusable(report)
    return LoadedPlan(plan, plan.metadata.plan_hash, None, "project")


def _unusable(report: Report) -> None:
    report.add(
        "PLA006",
        "the compile cache is not usable; compiling without it",
        hint="set ProjectConfig.cache_dir to a writable directory",
    )
