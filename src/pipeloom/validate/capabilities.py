"""L5: the plan against what the selected engine and catalog declare. No engine is imported."""

from __future__ import annotations

from pipeloom.config.bind import BoundPlugin, BoundProject
from pipeloom.config.resolve import ResolvedModel, ResolvedProject
from pipeloom.diagnostics import JsonValue, Report
from pipeloom.engine.types import CatalogCapabilities, EngineCapabilities, StateStoreCapabilities
from pipeloom.graph.dag import ProjectGraph
from pipeloom.validate.semantics import effective_strategy, where


def _check_model(
    project: ResolvedProject,
    m: ResolvedModel,
    engine: BoundPlugin,
    caps: EngineCapabilities,
    report: Report,
    no_views: BoundPlugin | None = None,
) -> None:
    c = m.config
    strategy = effective_strategy(c)
    ctx: dict[str, JsonValue] = {"model": m.name, "engine": engine.kind}

    def err(code: str, message: str, field: str, hint: str, sub: str = "") -> None:
        report.add(code, message, location=where(project, m, field, sub), hint=hint, context=ctx)

    if strategy is not None and strategy not in caps.strategies:
        err(
            "PLC501",
            f"engine '{engine.kind}' does not support strategy '{strategy}' (model '{m.name}')",
            "write",
            f"supported: {', '.join(sorted(caps.strategies)) or 'none'}",
            "/strategy",
        )
    if m.entry.sql is not None and not caps.sql_models:
        err(
            "PLC502",
            f"engine '{engine.kind}' cannot run SQL, but model '{m.name}' is SQL",
            "sql",
            "write the model in Python or choose an engine with SQL",
        )
    if strategy == "append" and c.retries > 0 and not caps.idempotent_writes:
        err(
            "PLC505",
            f"model '{m.name}' appends with retries on an engine without idempotent writes",
            "retries",
            "set retries to 0 or use another strategy",
        )
    if c.materialized == "view" and not caps.views:
        err(
            "PLC507",
            f"engine '{engine.kind}' has no catalog views (model '{m.name}')",
            "materialized",
            "use table, or an engine with views",
        )
    elif c.materialized == "view" and no_views is not None:
        err(
            "PLC507",
            f"catalog '{no_views.kind}' has no views (model '{m.name}')",
            "materialized",
            "use table, or a catalog with views",
        )
    if c.materialized != "view" and c.materialized not in caps.materializations:
        err(
            "PLC509",
            f"engine '{engine.kind}' cannot materialize '{c.materialized}' (model '{m.name}')",
            "materialized",
            f"supported: {', '.join(sorted(caps.materializations)) or 'none'}",
        )
    # A non-nullable contract column is counted with `evaluate` too (ADR-0044).
    checked = "expectations" if c.expectations else ""
    if not checked and any(not col.nullable for col in m.entry.columns or ()):
        checked = "columns"
    if checked and c.materialized not in ("view", "ephemeral") and not caps.quality:
        what = "expectations" if checked == "expectations" else "non-nullable contract columns"
        err(
            "PLC510",
            f"engine '{engine.kind}' cannot evaluate {what} (model '{m.name}')",
            checked,
            f"remove the {what} or use an engine with the quality capability",
        )
    if m.entry.python is not None and c.frame_api == "narwhals" and not caps.portable:
        err(
            "PLC508",
            f"frame_api narwhals needs a portable engine, not '{engine.kind}' (model '{m.name}')",
            "frame_api",
            "use frame_api: native",
        )


def _check_state(bound: BoundProject, report: Report) -> None:
    store = bound.plugins.get("state_stores")
    caps = None if store is None else store.spec.capabilities
    if store is None or not isinstance(caps, StateStoreCapabilities) or caps.shared:
        return
    if bound.runtime.boundary != "task":
        return
    pointers = bound.runtime.pointers
    pointer = f"{store.pointer}/kind" if "state" in pointers else pointers.get("boundary", "")
    report.add(
        "PLC503",
        f"boundary 'task' needs a shared state store, but '{store.kind}' is not shared",
        location=bound.loaded.project_file.doc.locate(pointer),
        hint="use a shared state store such as json, or boundary: process",
        context={"state": store.kind},
    )


def check_capabilities(pg: ProjectGraph, report: Report) -> None:
    """Report PLC501 to PLC509."""
    project = pg.project
    bound = project.bound
    _check_state(bound, report)
    engine, catalog = bound.plugins.get("engines"), bound.plugins.get("catalogs")
    if engine is None:
        return
    if catalog is not None:
        allowed = catalog.spec.compatible_engines
        if allowed and engine.kind not in allowed:
            doc = bound.loaded.project_file.doc
            report.add(
                "PLC504",
                f"catalog '{catalog.kind}' does not work with engine '{engine.kind}'",
                location=doc.locate(f"{catalog.pointer}/kind"),
                hint=f"compatible engines: {', '.join(sorted(allowed))}",
                context={"catalog": catalog.kind, "engine": engine.kind},
            )
    caps = engine.spec.capabilities
    if not isinstance(caps, EngineCapabilities):
        return
    cat_caps = None if catalog is None else catalog.spec.capabilities
    no_views = isinstance(cat_caps, CatalogCapabilities) and not cat_caps.views
    for s in project.sources.values():
        if s.config.format is not None and s.config.format not in caps.formats:
            report.add(
                "PLC506",
                f"source '{s.name}' has format '{s.config.format}', "
                f"which engine '{engine.kind}' cannot read",
                location=project.locate(s.file, f"{s.pointer}/format"),
                hint=f"supported formats: {', '.join(sorted(caps.formats)) or 'none'}",
                context={"source": s.name, "format": s.config.format},
            )
    for m in project.models.values():
        _check_model(project, m, engine, caps, report, catalog if no_views else None)
