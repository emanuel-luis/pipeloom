"""Load a project: L0 (parse) and L1 (schema) for pipeloom.yml, model files and defaults."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, ValidationError

from pipeloom.config.discover import PROJECT_FILE, find_model_files, read_file
from pipeloom.config.errors import from_validation_error
from pipeloom.config.models import DefaultsFile, ModelFile, ProjectConfig
from pipeloom.config.yaml import LoadedDocument, parse_yaml
from pipeloom.diagnostics import Report
from pipeloom.secrets.interpolation import InterpolationRecord, interpolate

M = TypeVar("M", bound=BaseModel)

_SECRET_KEY = re.compile(
    r"password|passwd|secret|token|api_?key|private_?key|credential|access_?key", re.IGNORECASE
)
_REFERENCE = re.compile(r"^\$\{(?:secret|env):[^}]*\}$")


@dataclass(frozen=True)
class ProjectOptions:
    project_dir: Path
    target: str | None = None
    vars: Mapping[str, Any] = field(default_factory=dict)
    boundary: Literal["process", "task"] | None = None
    plugins: tuple[str, ...] = ()
    env: Mapping[str, str] | None = None


@dataclass(frozen=True)
class LoadedFile(Generic[M]):
    path: str
    doc: LoadedDocument
    config: M


@dataclass(frozen=True)
class LoadedProject:
    project_dir: Path
    config: ProjectConfig
    project_file: LoadedFile[ProjectConfig]
    model_files: tuple[LoadedFile[ModelFile], ...]
    defaults_files: tuple[LoadedFile[DefaultsFile], ...]
    vars: Mapping[str, Any]
    interpolations: tuple[InterpolationRecord, ...]


def _unescape(segment: str) -> str:
    return segment.replace("~1", "/").replace("~0", "~")


def _secret_key_of(pointer: str) -> str | None:
    """The nearest key in a pointer that looks like a secret field, else None."""
    for seg in reversed(pointer.split("/")[1:]):
        if seg.isdigit():
            continue
        key = _unescape(seg)
        return key if _SECRET_KEY.search(key) else None
    return None


def _in_vars(pointer: str) -> bool:
    return pointer == "/vars" or pointer.startswith("/vars/")


def _scan_secrets(doc: LoadedDocument, report: Report) -> None:
    """PLS104: a literal string under a secret-like key."""

    def literal(value: Any) -> bool:
        return isinstance(value, str) and value != "" and not _REFERENCE.match(value)

    def visit(node: Any, pointer: str, key: str | None) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                visit(v, f"{pointer}/{k.replace('~', '~0').replace('/', '~1')}", k)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                visit(v, f"{pointer}/{i}", key)
        elif key is not None and _SECRET_KEY.search(key) and literal(node):
            report.add(
                "PLS104",
                f"literal value in secret-like field '{key}'",
                location=doc.locate(pointer),
                hint="use ${secret:provider/path} or ${env:NAME}",
            )

    visit(doc.data, "", None)


def _validate(
    model: type[M], data: Any, doc: LoadedDocument, report: Report
) -> LoadedFile[M] | None:
    try:
        return LoadedFile(doc.path, doc, model.model_validate(data))
    except ValidationError as err:
        from_validation_error(err, doc, model, report)
        return None


def _read(project_dir: Path, rel: str, report: Report) -> LoadedDocument | None:
    raw = read_file(project_dir, rel, report)
    return None if raw is None else parse_yaml(raw, rel, report)


def load_project(opts: ProjectOptions, report: Report) -> LoadedProject | None:
    """Load everything at L0 and L1. Returns None when any error was reported."""
    env = os.environ if opts.env is None else opts.env
    before = report.error_count
    project_dir = opts.project_dir

    doc = _read(project_dir, PROJECT_FILE, report)
    if doc is None:
        return None
    if not isinstance(doc.data, dict):
        report.add("PLS102", "pipeloom.yml must be a mapping", location=doc.locate(""))
        return None
    if doc.data.get("config_version") != 1 or isinstance(doc.data.get("config_version"), bool):
        report.add(
            "PLS103",
            "config_version is missing or not 1",
            location=doc.locate("/config_version", key=True),
            hint="start the file with 'config_version: 1'",
        )
        return None
    _scan_secrets(doc, report)

    def skip_secret(pointer: str) -> bool:
        return _secret_key_of(pointer) is not None

    # The vars block sees env only; --vars is laid over it; then everything else sees both.
    block, records = interpolate(
        doc,
        env=env,
        vars={},
        report=report,
        skip=lambda p: not _in_vars(p) or skip_secret(p),
    )
    raw_vars = block.get("vars") if isinstance(block, dict) else None
    merged: dict[str, Any] = {**(raw_vars if isinstance(raw_vars, dict) else {}), **opts.vars}
    data, more = interpolate(
        doc, env=env, vars=merged, report=report, skip=lambda p: _in_vars(p) or skip_secret(p)
    )
    records += more
    if isinstance(raw_vars, dict):
        data["vars"] = merged

    project_file = _validate(ProjectConfig, data, doc, report)
    if project_file is None:
        return None
    config = project_file.config

    files = find_model_files(project_dir, config.model_paths, doc, report)
    model_files: list[LoadedFile[ModelFile]] = []
    defaults_files: list[LoadedFile[DefaultsFile]] = []
    for rel in sorted(files.models + files.defaults):
        mdoc = _read(project_dir, rel, report)
        if mdoc is None:
            continue
        _scan_secrets(mdoc, report)
        mdata, mrecords = interpolate(mdoc, env=env, vars=merged, report=report, skip=skip_secret)
        records += mrecords
        if rel in files.defaults:
            d = _validate(DefaultsFile, mdata, mdoc, report)
            if d is not None:
                defaults_files.append(d)
        else:
            m = _validate(ModelFile, mdata, mdoc, report)
            if m is not None:
                model_files.append(m)

    if report.error_count > before:
        return None
    return LoadedProject(
        project_dir=project_dir,
        config=config,
        project_file=project_file,
        model_files=tuple(model_files),
        defaults_files=tuple(defaults_files),
        vars=merged,
        interpolations=tuple(records),
    )
