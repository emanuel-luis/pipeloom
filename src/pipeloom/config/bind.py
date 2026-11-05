"""Bind a loaded project to a target: pick the target, merge runtime settings, bind plugins.

Binding checks each plugin block against the plugin's own options model, so only the plugins a
config names are imported. Building the plugins (calling their factories) is left to the caller.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ValidationError

from pipeloom.config.errors import from_validation_error
from pipeloom.config.load import LoadedProject, ProjectOptions
from pipeloom.config.models import ArtifactsConfig, PluginBlock
from pipeloom.diagnostics import Report, suggest
from pipeloom.plugins import Group, PluginRegistry, PluginSpec
from pipeloom.secrets.resolve import SecretError, SecretResolver

TARGET_ENV = "PIPELOOM_TARGET"
DEFAULT_WORKERS = 4

# Runtime keys whose block names a plugin, and the plugin group each one selects from.
_BLOCK_GROUPS: Mapping[str, Group] = {
    "engine": "engines",
    "catalog": "catalogs",
    "naming": "naming",
    "state": "state_stores",
    "deploy": "renderers",
}


@dataclass(frozen=True)
class BoundPlugin:
    """A plugin block checked against its spec. `create()` builds the plugin."""

    group: Group
    kind: str
    spec: PluginSpec[Any]
    options: BaseModel
    pointer: str
    """Where the block sits in pipeloom.yml, as a JSON pointer."""
    raw: Mapping[str, Any]
    """The block's options as written, with `${secret:...}` references still in place."""
    project_dir: Path
    literal_refs: Mapping[str, tuple[tuple[int, int], ...]]
    """Spans of the `${secret:...}` references written literally in the YAML, by JSON pointer,
    under this block. Only these resolve."""

    def create(self, secrets: SecretResolver | None = None) -> Any:
        """Build the plugin. With `secrets`, references in the options are resolved first, in
        memory only: that is the execution path. Raises SecretError."""
        if secrets is None or not self.literal_refs:
            return self.spec.factory(self.options)
        resolved = secrets.resolve(dict(self.raw), pointer=self.pointer, literal=self.literal_refs)
        assert isinstance(resolved, dict)
        try:
            options = self.spec.config_model.model_validate(
                resolved, context={"project_dir": self.project_dir}
            )
        except ValidationError as err:  # never echo the input: it holds the secret
            fields = ", ".join(sorted({".".join(str(p) for p in e["loc"]) for e in err.errors()}))
            raise SecretError(
                "", "", f"{self.group} '{self.kind}' options are invalid once resolved: {fields}"
            ) from None
        try:
            return self.spec.factory(options)
        except Exception as exc:  # noqa: BLE001 - plugin code may repeat a resolved option
            # No chained traceback: its frames and message could hold the resolved value.
            reason = secrets.redactor.mask(f"{type(exc).__name__}: {exc}")
            raise SecretError(
                "", "", f"{self.group} '{self.kind}' could not be built: {reason}"
            ) from None


@dataclass(frozen=True)
class RuntimeSettingsResolved:
    engine: PluginBlock | None
    catalog: PluginBlock | None
    naming: PluginBlock
    state: PluginBlock
    deploy: PluginBlock | None
    """The orchestrator deployment block; bound against `renderers`, never built by the core."""
    workers: int
    boundary: Literal["process", "task"]
    artifacts: ArtifactsConfig
    cancel_grace: int | None
    """Seconds; None when no setting names it, so the run uses its default."""
    pointers: Mapping[str, str]
    """Where each explicitly set key came from, as JSON pointers."""


@dataclass(frozen=True)
class BoundProject:
    loaded: LoadedProject
    target: str
    runtime: RuntimeSettingsResolved
    plugins: Mapping[Group, BoundPlugin]
    """Engine and catalog when set; naming and the state store always; `renderers` when the
    target has a deploy block (the core never calls its factory)."""
    sinks: tuple[BoundPlugin, ...]


def _select_target(
    loaded: LoadedProject, opts: ProjectOptions, env: Mapping[str, str], report: Report
) -> str | None:
    """--target, then PIPELOOM_TARGET, then default_target, then the only target."""
    doc = loaded.project_file.doc
    targets = loaded.config.targets
    chosen: tuple[str, str] | None = None
    if opts.target:
        chosen = (opts.target, "/targets")
    elif env.get(TARGET_ENV):
        chosen = (env[TARGET_ENV], "/targets")
    elif loaded.config.default_target:
        chosen = (loaded.config.default_target, "/default_target")
    if chosen is not None:
        name, pointer = chosen
        if name in targets:
            return name
        report.add(
            "PLR204",
            f"unknown target '{name}'",
            location=doc.locate(pointer),
            hint=suggest(name, targets) or f"targets: {', '.join(sorted(targets))}",
            context={"target": name},
        )
        return None
    if len(targets) == 1:
        return next(iter(targets))
    report.add(
        "PLR207",
        "no target selected",
        location=doc.locate("/targets"),
        hint=f"use --target, PIPELOOM_TARGET or default_target; targets: {', '.join(sorted(targets))}",
    )
    return None


def _resolve_runtime(
    loaded: LoadedProject, target: str, opts: ProjectOptions
) -> RuntimeSettingsResolved:
    """Top-level keys, then targets.<t>, then CLI flags. A key set lower replaces it whole."""
    top = loaded.config
    tgt = loaded.config.targets[target]
    values: dict[str, Any] = {}
    pointers: dict[str, str] = {}
    for key in type(top).model_fields.keys() & type(tgt).model_fields.keys():
        if getattr(tgt, key) is not None:
            values[key], pointers[key] = getattr(tgt, key), f"/targets/{target}/{key}"
        elif getattr(top, key, None) is not None:
            values[key], pointers[key] = getattr(top, key), f"/{key}"
    if opts.boundary is not None:
        values["boundary"] = opts.boundary
        pointers.pop("boundary", None)
    return RuntimeSettingsResolved(
        engine=values.get("engine"),
        catalog=values.get("catalog"),
        naming=values.get("naming") or PluginBlock(kind="none"),
        state=values.get("state") or PluginBlock(kind="sqlite"),
        deploy=values.get("deploy"),
        workers=values.get("workers") or DEFAULT_WORKERS,
        boundary=values.get("boundary") or "process",
        artifacts=values.get("artifacts") or ArtifactsConfig(),
        cancel_grace=values.get("cancel_grace"),
        pointers=pointers,
    )


def _bind_block(
    loaded: LoadedProject,
    registry: PluginRegistry,
    report: Report,
    group: Group,
    block: PluginBlock,
    pointer: str,
) -> BoundPlugin | None:
    doc = loaded.project_file.doc
    where = doc.locate(f"{pointer}/kind")
    known = sorted({e.kind for e in registry.entries(group)})
    if block.kind not in known:
        report.add(
            "PLR204",
            f"unknown {group.removesuffix('s')} kind '{block.kind}'",
            location=where,
            hint=suggest(block.kind, known) or f"available: {', '.join(known) or 'none'}",
            context={"group": group, "kind": block.kind},
        )
        return None
    spec = registry.load(group, block.kind, report, where)
    if spec is None:
        return None
    try:
        options = spec.config_model.model_validate(
            dict(block.model_extra or {}), context={"project_dir": loaded.project_dir}
        )
    except ValidationError as err:
        from_validation_error(err, doc, spec.config_model, report, base_pointer=pointer)
        return None
    raw = dict(block.model_extra or {})
    literal: dict[str, list[tuple[int, int]]] = {}
    for r in loaded.interpolations:
        if r.kind == "secret" and r.span is not None and r.pointer.startswith(pointer + "/"):
            literal.setdefault(r.pointer, []).append(r.span)
    refs = {p: tuple(s) for p, s in literal.items()}
    return BoundPlugin(group, block.kind, spec, options, pointer, raw, loaded.project_dir, refs)


def bind_project(
    loaded: LoadedProject, opts: ProjectOptions, registry: PluginRegistry, report: Report
) -> BoundProject | None:
    """Select the target and bind its plugins. None when any error was reported."""
    env = os.environ if opts.env is None else opts.env
    target = _select_target(loaded, opts, env, report)
    if target is None:
        return None
    runtime = _resolve_runtime(loaded, target, opts)
    before = report.error_count
    plugins: dict[Group, BoundPlugin] = {}
    for key, group in _BLOCK_GROUPS.items():
        block: PluginBlock | None = getattr(runtime, key)
        if block is None:
            continue
        pointer = runtime.pointers.get(key, f"/{key}")
        bound = _bind_block(loaded, registry, report, group, block, pointer)
        if bound is not None:
            plugins[group] = bound
    sinks: list[BoundPlugin] = []
    for i, sink in enumerate(loaded.config.events.sinks):
        bound = _bind_block(loaded, registry, report, "sinks", sink, f"/events/sinks/{i}")
        if bound is not None:
            sinks.append(bound)
    if report.error_count > before:
        return None
    return BoundProject(loaded, target, runtime, plugins, tuple(sinks))


__all__ = ["BoundPlugin", "BoundProject", "RuntimeSettingsResolved", "bind_project"]
