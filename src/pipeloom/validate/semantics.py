"""L4: plan semantics. Options that contradict each other or the materialization."""

from __future__ import annotations

from pipeloom.config.models import ResolvedModelConfig
from pipeloom.config.resolve import ResolvedModel, ResolvedProject
from pipeloom.diagnostics import JsonValue, Location, Report
from pipeloom.engine.types import SCD2_COLUMNS
from pipeloom.graph.dag import ProjectGraph, inlined_ancestors

PERSISTED = ("table", "incremental", "intermediate")
_KEYED = ("merge", "scd2")
_NO_ROWS = ("view", "ephemeral")
_AGGREGATE_CHECKS = ("unique", "row_count", "freshness")
_SCD2 = tuple(c.name for c in SCD2_COLUMNS)


def effective_strategy(config: ResolvedModelConfig) -> str | None:
    """The write strategy a persisted model uses; None for views and ephemeral models."""
    if config.materialized not in PERSISTED:
        return None
    if config.write.strategy:
        return config.write.strategy
    return "append" if config.materialized == "incremental" else "overwrite"


def _lookback_append(m: ResolvedModel) -> bool:
    c = m.config
    return (
        c.materialized == "incremental"
        and effective_strategy(c) == "append"
        and c.incremental is not None
        and c.incremental.lookback > 0
    )


def _scd2_problems(m: ResolvedModel) -> list[str]:
    """PLP416: what an scd2 model sets that conflicts with the strategy (ADR-0008 amendment)."""
    c, w = m.config, m.config.write
    out: list[str] = []
    named = {
        "unique_key": w.unique_key or [],
        "tracked_columns": w.tracked_columns or [],
        "partition_by": c.partition_by,
        "cluster_by": c.cluster_by,
        "deletes.column": [w.deletes.column] if w.deletes else [],
        "columns": [col.name for col in m.entry.columns or []],
    }
    out += [
        f"{opt} names system column '{n}'" for opt, ns in named.items() for n in ns if n in _SCD2
    ]
    if w.tracked_columns is not None and not w.tracked_columns:
        out.append("tracked_columns is empty")
    overlap = [n for n in w.tracked_columns or [] if n in (w.unique_key or [])]
    out += [f"tracked column '{n}' is part of unique_key" for n in overlap]
    if w.deletes is not None and w.deletes.mode != "hard":
        out.append("deletes.mode is soft")
    return out


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
    if _lookback_append(m):
        err(
            "PLP410",
            f"incremental model '{m.name}' appends with a lookback, so it reloads rows it wrote",
            "incremental",
            "use write.strategy: merge with a unique_key, or set lookback to 0",
            "/lookback",
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
    if c.materialized == "intermediate" and (
        "incremental" in m.own_fields
        or c.write.strategy not in (None, "overwrite")
        or c.partition_by
    ):
        err(
            "PLP413",
            f"intermediate model '{m.name}' sets incremental, a strategy other than overwrite "
            "or partition_by",
            "materialized",
            "remove those options, or materialize the model as a table",
        )
    if "expectations" in m.own_fields and c.expectations and c.materialized in _NO_ROWS:
        err(
            "PLP411",
            f"{c.materialized} model '{m.name}' cannot have expectations",
            "expectations",
            "move the expectations to a table that reads it",
        )
    if m.entry.columns is not None and c.materialized in _NO_ROWS:
        err(
            "PLP414",
            f"{c.materialized} model '{m.name}' cannot have a contract (columns)",
            "columns",
            "move the columns to a table that reads it, or materialize the model as a table",
        )
    if strategy == "replace_partitions" and not c.partition_by:
        err(
            "PLP407",
            f"model '{m.name}' uses replace_partitions without partition_by",
            "partition_by",
            "set partition_by to the partition columns",
        )
    if strategy == "scd2":
        problems = _scd2_problems(m)
        if problems:
            err(
                "PLP416",
                f"scd2 options of model '{m.name}' conflict: {'; '.join(problems)}",
                "write",
                f"the columns {', '.join(_SCD2)} are added by scd2; tracked_columns must be "
                "non-empty and outside unique_key, and deletes must use mode: hard",
            )
    # Only what the entry itself declares can be a mistake: defaults reach every model.
    declared = m.entry.write.model_fields_set if m.entry.write is not None else frozenset[str]()
    unused: list[tuple[str, str]] = []  # (option, pointer below the model)
    if strategy is None:  # a view or an ephemeral model: nothing is written by a strategy
        if m.entry.write is not None:
            unused.append(("write", "write"))
        unused += [(n, n) for n in ("partition_by", "cluster_by") if n in m.own_fields]
    else:
        keyed: dict[str, tuple[str, ...]] = {"unique_key": _KEYED, "deletes": _KEYED}
        keyed |= {"update_condition": ("merge",), "tracked_columns": ("scd2",)}
        unused += [
            (n, f"write/{n}") for n, ok in keyed.items() if n in declared and strategy not in ok
        ]
    if "incremental" in m.own_fields and c.materialized != "incremental":
        unused.append(("incremental", "incremental"))
    for option, pointer in unused:
        what = f"strategy '{strategy}'" if strategy else f"materialized '{c.materialized}'"
        field, _, sub = pointer.partition("/")
        err(
            "PLP405",
            f"option '{option}' of '{m.name}' is not applied by {what}",
            field,
            f"remove '{option}' or change the strategy or materialization",
            f"/{sub}" if sub else "",
        )


def _check_view(
    project: ResolvedProject, m: ResolvedModel, inlined: list[str], report: Report
) -> None:
    """PLP409: a view stores its SQL as written, so it cannot be Python or inline an ephemeral."""
    if m.entry.sql is None:
        why = "is a Python model"
        hint = "write the view as SQL, or materialize it as a table"
    elif inlined:
        why = f"reads ephemeral model(s) {', '.join(inlined)}"
        hint = "make those models tables, or materialize the view as a table"
    else:
        return
    report.add(
        "PLP409",
        f"view '{m.name}' {why}",
        location=where(project, m, "materialized"),
        hint=hint,
        context={"model": m.name},
    )


def check_semantics(pg: ProjectGraph, report: Report) -> None:
    """Report PLP401 to PLP409 for every model."""
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
    # The effective catalog and schema: the model's own, else the naming options'. Prefix and
    # per_user transform both sides of a comparison equally, so they need not be applied.
    naming = project.bound.plugins["naming"].options
    base_catalog = getattr(naming, "catalog", None)
    base_schema = getattr(naming, "schema_", None)

    def place(c: ResolvedModelConfig) -> tuple[str | None, str | None]:
        return (c.catalog or base_catalog, c.schema_ or base_schema)

    taken = {(*place(mm.config), mm.name) for mm in project.models.values()}
    for uid, m in project.models.items():
        _check_model(project, m, report)
        clash = f"{m.name}__quarantine"
        if (
            m.config.quality.quarantine
            and m.config.expectations
            and (*place(m.config), clash) in taken
        ):
            report.add(
                "PLP412",
                f"the quarantine table of '{m.name}' collides with model '{clash}'",
                location=where(project, m, "quality"),
                hint=f"rename model '{clash}' or turn quality.quarantine off",
                context={"model": m.name},
            )
        if m.config.materialized == "view":
            _check_view(
                project,
                m,
                sorted(project.models[e].name for e in inlined_ancestors(graph, uid, ephemeral)),
                report,
            )
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
