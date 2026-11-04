"""L4: plan semantics. Options that contradict each other or the materialization."""

from __future__ import annotations

from pipeloom.config.models import ResolvedModelConfig
from pipeloom.config.resolve import ResolvedModel, ResolvedProject
from pipeloom.diagnostics import JsonValue, Location, Report
from pipeloom.graph.dag import ProjectGraph, inlined_ancestors

PERSISTED = ("table", "incremental", "intermediate")
_KEYED = ("merge", "scd2")
_AGGREGATE_CHECKS = ("unique", "row_count", "freshness")


def effective_strategy(config: ResolvedModelConfig) -> str | None:
    """The write strategy a persisted model uses; None for views and ephemeral models."""
    if config.materialized not in PERSISTED:
        return None
    if config.write.strategy:
        return config.write.strategy
    return "append" if config.materialized == "incremental" else "overwrite"


def where(project: ResolvedProject, m: ResolvedModel, field: str, sub: str = "") -> Location:
    """The model's own `field` (and `sub` pointer below it) when it sets one, else its name."""
    if field in m.own_fields:
        return project.locate(m.file, f"{m.pointer}/{field}{sub}")
    return project.locate_model(m)


def _check_model(project: ResolvedProject, m: ResolvedModel, report: Report) -> None:
    c = m.config
    w = c.write
    strategy = effective_strategy(c)
    ctx: dict[str, JsonValue] = {"model": m.name}

    def err(code: str, message: str, field: str, hint: str, sub: str = "") -> None:
        report.add(code, message, location=where(project, m, field, sub), hint=hint, context=ctx)

    if strategy in _KEYED and not w.unique_key:
        err(
            "PLP401",
            f"model '{m.name}' uses {strategy} without write.unique_key",
            "write",
            "set write.unique_key to the columns that identify a row",
            "/unique_key",
        )
    if c.materialized == "incremental" and (c.incremental is None or c.incremental.column is None):
        err(
            "PLP402",
            f"incremental model '{m.name}' has no incremental.column",
            "incremental",
            "set incremental.column to the column compared with the watermark",
            "/column",
        )
    if c.incremental is not None and c.incremental.input not in m.inputs:
        err(
            "PLP404",
            f"incremental.input '{c.incremental.input}' is not an input of '{m.name}'",
            "incremental",
            f"inputs: {', '.join(m.inputs) or 'none'}",
            "/input",
        )
    for i, e in enumerate(c.expectations):
        aggregate = e.check is not None and any(
            getattr(e.check, k) is not None for k in _AGGREGATE_CHECKS
        )
        if aggregate and e.on_violation == "drop":
            err(
                "PLP403",
                f"expectation '{e.name}' of '{m.name}' is an aggregate check and cannot drop",
                "expectations",
                "use on_violation: warn or fail",
                f"/{i}/on_violation",
            )
    if strategy == "replace_partitions" and not c.partition_by:
        err(
            "PLP407",
            f"model '{m.name}' uses replace_partitions without partition_by",
            "partition_by",
            "set partition_by to the partition columns",
        )
    unused: list[tuple[str, str]] = []  # (option, field to point at)
    if c.materialized == "ephemeral" and w.model_fields_set:
        unused.append(("write", "write"))
    if strategy is not None:
        if strategy not in _KEYED:
            unused += [(n, "write") for n in ("unique_key", "deletes") if getattr(w, n) is not None]
        if strategy != "merge" and w.update_condition is not None:
            unused.append(("update_condition", "write"))
        if strategy != "scd2" and w.tracked_columns is not None:
            unused.append(("tracked_columns", "write"))
    if c.incremental is not None and c.materialized != "incremental":
        unused.append(("incremental", "incremental"))
    for option, field in unused:
        what = f"strategy '{strategy}'" if strategy else f"materialized '{c.materialized}'"
        err(
            "PLP405",
            f"option '{option}' of '{m.name}' is not applied by {what}",
            field,
            f"remove '{option}' or change the strategy or materialization",
        )


def check_semantics(pg: ProjectGraph, report: Report) -> None:
    """Report PLP401 to PLP408 for every model."""
    project, graph = pg.project, pg.graph
    ephemeral = frozenset(
        u for u, m in project.models.items() if m.config.materialized == "ephemeral"
    )
    fanout = dict.fromkeys(ephemeral, 0)
    for uid in project.models:
        if uid not in ephemeral:
            for e in inlined_ancestors(graph, uid, ephemeral):
                fanout[e] += 1
    limit = project.bound.loaded.config.ephemeral_fanout_warn
    for uid, m in project.models.items():
        _check_model(project, m, report)
        if uid in ephemeral and fanout[uid] == 0:
            report.add(
                "PLP408",
                f"ephemeral model '{m.name}' has no consumer",
                location=project.locate_model(m),
                hint="no persisted model reads it, so it never runs",
                context={"model": m.name},
            )
        elif uid in ephemeral and fanout[uid] > limit:
            report.add(
                "PLP406",
                f"ephemeral model '{m.name}' is inlined in {fanout[uid]} units (limit {limit})",
                location=project.locate_model(m, "materialized"),
                hint="make it 'intermediate' to compute it once",
                context={"model": m.name, "units": fanout[uid]},
            )
