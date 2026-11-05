"""Compile a validated project into a Plan."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Literal

from pipeloom import __version__
from pipeloom.config.models import ResolvedModelConfig
from pipeloom.diagnostics import Report
from pipeloom.engine.protocols import NamingStrategy
from pipeloom.engine.types import TargetRef
from pipeloom.graph.dag import ProjectGraph
from pipeloom.graph.selectors import Selection, SelectorError, apply
from pipeloom.plan.manifest import compute_plan_hash, node_checksum
from pipeloom.plan.model import (
    EnvVar,
    Plan,
    PlanMetadata,
    PlanNode,
    PlanSelection,
    PlanSource,
    PluginInfo,
    PythonBody,
    SqlBody,
)
from pipeloom.plan.units import build_units
from pipeloom.platform import Clock, Identity
from pipeloom.plugins import PluginRegistry
from pipeloom.validate.semantics import effective_strategy
from pipeloom.validate.static import find_module


def _relation(
    naming: NamingStrategy, name: str, config: ResolvedModelConfig, identity: Identity
) -> TargetRef:
    """The naming strategy's relation; a model catalog replaces the strategy's catalog."""
    ref = naming.relation(name, config.schema_, identity)
    return ref if config.catalog is None else replace(ref, catalog=config.catalog)


def compile_plan(
    pg: ProjectGraph,
    *,
    selection: Selection,
    boundary: Literal["process", "task"] | None,
    registry: PluginRegistry,
    identity: Identity,
    clock: Clock,
    invocation_id: str,
    report: Report,
) -> Plan | None:
    """Build the plan of a project that passed L0 to L6.

    Units and maps always cover the whole graph; `selection` only fills `selection.selected`.
    Returns None when the selector is invalid (its diagnostic goes to `report`).
    """
    project, graph = pg.project, pg.graph
    bound = project.bound
    loaded = bound.loaded
    try:
        selected = apply(selection, graph, project, report)
    except SelectorError as err:
        report.extend([err.diagnostic])
        return None

    units = build_units(project, graph)
    unit_ids: dict[str, list[str]] = {uid: [] for uid in project.models}
    for unit in units.values():
        for uid in unit.nodes:
            unit_ids[uid].append(unit.unit_id)

    naming: NamingStrategy = bound.plugins["naming"].create()
    nodes: dict[str, PlanNode] = {}
    for uid, m in project.models.items():
        body: SqlBody | PythonBody
        if m.entry.sql is not None:
            body = SqlBody(sql=m.entry.sql)
            raw = m.entry.sql.encode("utf-8")
        else:
            ref = m.entry.python or ""
            file = find_module(loaded.project_dir, loaded.config.python_paths, ref.split(":")[0])
            if file is None:
                raise LookupError(f"module of '{ref}' not found; validate before compiling")
            body = PythonBody(callable=ref, file=file)
            raw = ref.encode("utf-8") + b"\0" + (loaded.project_dir / file).read_bytes()
        persisted = m.config.materialized != "ephemeral"
        # The strategy a run applies is decided here, so later versions only read it.
        config = m.config.model_copy(
            update={
                "write": m.config.write.model_copy(
                    update={"strategy": effective_strategy(m.config)}
                )
            }
        )
        nodes[uid] = PlanNode(
            unique_id=uid,
            name=m.name,
            file=m.file,
            line=project.locate(m.file, f"{m.pointer}/name").line,
            materialized=m.config.materialized,
            body=body,
            inputs={alias: b.unique_id for alias, b in m.inputs.items()},
            relation=_relation(naming, m.name, m.config, identity) if persisted else None,
            unit_ids=tuple(sorted(unit_ids[uid])),
            config=config,
            tags=tuple(m.config.tags),
            checksum=node_checksum(config, raw),
        )
    sources = {
        uid: PlanSource(
            unique_id=uid,
            name=s.name,
            file=s.file,
            format=s.config.format,
            location=s.config.location,
            table=s.config.table,
            options=s.config.options,
            tags=tuple(s.config.tags),
        )
        for uid, s in project.sources.items()
    }

    plugins: dict[str, PluginInfo] = {}
    for group, plugin in sorted(bound.plugins.items()):
        entry = next((e for e in registry.entries(group) if e.kind == plugin.kind), None)
        plugins[group] = PluginInfo(
            kind=plugin.kind,
            distribution=entry.distribution if entry else "",
            version=entry.version if entry else "",
        )
    renderer = bound.plugins.get("renderers")
    env_vars = sorted({(r.name, r.default_used) for r in project.interpolations if r.kind == "env"})
    metadata: dict[str, Any] = {
        "pipeloom_version": __version__,
        "plugins": plugins,
        "config_version": loaded.config.config_version,
        "plan_hash": "",
        "project": project.name,
        "target": bound.target,
        "boundary": boundary or bound.runtime.boundary,
        "python_paths": tuple(loaded.config.python_paths),
        "vars": dict(loaded.vars),
        "env_vars": tuple(EnvVar(name=n, default_used=d) for n, d in env_vars),
        "generated_at": clock.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "invocation_id": invocation_id,
        "deploy": None
        if renderer is None
        else {"kind": renderer.kind, **renderer.options.model_dump(mode="json")},
    }
    plan = Plan(
        metadata=PlanMetadata(**metadata),
        nodes=dict(sorted(nodes.items())),
        sources=dict(sorted(sources.items())),
        units=units,
        parent_map=dict(sorted(graph.parents.items())),
        child_map=dict(sorted(graph.children.items())),
        selection=PlanSelection(
            select=selection.select, exclude=selection.exclude, selected=tuple(sorted(selected))
        ),
    )
    digest = compute_plan_hash(plan)
    return plan.model_copy(
        update={"metadata": plan.metadata.model_copy(update={"plan_hash": digest})}
    )
