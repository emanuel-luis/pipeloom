"""L5: the plan against what the selected engine and catalog declare. No engine is imported."""

from __future__ import annotations

from pipeloom.config.bind import BoundPlugin
from pipeloom.config.resolve import ResolvedModel, ResolvedProject
from pipeloom.diagnostics import JsonValue, Report
from pipeloom.engine.types import EngineCapabilities
from pipeloom.graph.dag import ProjectGraph
from pipeloom.validate.semantics import effective_strategy, where


def _check_model(
    project: ResolvedProject,
    m: ResolvedModel,
    engine: BoundPlugin,
    caps: EngineCapabilities,
    report: Report,
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
    if m.entry.python is not None and c.frame_api == "narwhals" and not caps.portable:
        err(
            "PLC508",
            f"frame_api narwhals needs a portable engine, not '{engine.kind}' (model '{m.name}')",
            "frame_api",
            "use frame_api: native",
        )


def check_capabilities(pg: ProjectGraph, report: Report) -> None:
    """Report PLC501, PLC502, PLC504, PLC505, PLC506, PLC507 and PLC508."""
    project = pg.project
    bound = project.bound
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
        _check_model(project, m, engine, caps, report)
