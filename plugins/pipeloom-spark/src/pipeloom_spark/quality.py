"""Expectations on Spark frames, with the 0.3 semantics: a null input passes every row check
except not_null, NaN reads as null, and the core judges the counts.

Row checks are SQL text (pure, testable without Spark) turned into one boolean column each, and
`evaluate` gets every count and aggregate from a single `agg` job.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from pipeloom.engine.types import (
    ExpectationSpec,
    QualityCounts,
    QuarantineMeta,
    Relation,
    Schema,
)
from pipeloom.quality.quarantine import FAILED_COLUMN, RUN_ID_COLUMN, TS_COLUMN
from pipeloom_spark.sqltext import literal, quote_ident

ROW_CHECKS: Final = frozenset({"not_null", "accepted_values", "range", "regex", "expr"})
_V: Final = "__pipeloom_v"
_RAW: Final = "__pipeloom_raw"
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)


def _dtypes(schema: Schema) -> dict[str, str]:
    return {c.name: c.dtype for c in schema}


def _ref(column: str, dtype: str) -> str:
    """The column, with NaN read as null for float columns."""
    q = quote_ident(column)
    return f"CASE WHEN isnan({q}) THEN NULL ELSE {q} END" if dtype.startswith("float") else q


def violation_sql(spec: ExpectationSpec, schema: Schema) -> str:
    """A non-null boolean SQL expression: true when the row violates the row check."""
    if spec.kind == "expr":
        return f"coalesce(NOT ({spec.params['expr']}), false)"
    types = _dtypes(schema)
    parts = []
    for column in spec.columns:
        dtype = types[column]
        ref = _ref(column, dtype)
        if spec.kind == "not_null":
            parts.append(f"({ref}) IS NULL")
            continue
        if spec.kind == "accepted_values":
            values = spec.params["values"]
            assert isinstance(values, list)
            bad = f"NOT (({ref}) IN ({', '.join(literal(v, dtype) for v in values)}))"
        elif spec.kind == "range":
            low, high = spec.params.get("min"), spec.params.get("max")
            bounds = [f"({ref}) < {literal(low, dtype)}"] if low is not None else []
            bounds += [f"({ref}) > {literal(high, dtype)}"] if high is not None else []
            bad = f"({' OR '.join(bounds)})" if bounds else "false"
        elif spec.kind == "regex":
            pattern = literal(str(spec.params["pattern"]), "string")
            bad = f"NOT (CAST(({ref}) AS STRING) RLIKE {pattern})"
        else:
            raise ValueError(f"not a row check: {spec.kind}")
        parts.append(f"(({ref}) IS NOT NULL AND {bad})")
    return f"coalesce({' OR '.join(parts) or 'false'}, false)"


def violation_name(i: int) -> str:
    return f"{_V}{i}"


def row_checks(specs: Sequence[ExpectationSpec]) -> list[ExpectationSpec]:
    return [s for s in specs if s.kind in ROW_CHECKS]


def aggregate_sql(specs: Sequence[ExpectationSpec], schema: Schema) -> list[str]:
    """The `agg` expressions over a frame marked by `violation_columns` for the row checks of
    `specs`: rows, failed per row check, dropped (only with a drop check), then one per aggregate
    check in order."""
    rows = row_checks(specs)
    out = ["count(1)"]
    out += [f"count_if({violation_name(i)})" for i in range(len(rows))]
    drops = [violation_name(i) for i, s in enumerate(rows) if s.action == "drop"]
    if drops:
        out.append(f"count_if({' OR '.join(drops)})")
    types = _dtypes(schema)
    for spec in specs:
        if spec.kind == "row_count":
            out.append("count(1)")
        elif spec.kind == "unique":
            refs = [_ref(c, types[c]) for c in spec.columns]
            present = " AND ".join(f"({r}) IS NOT NULL" for r in refs)
            out.append(f"count_if({present}) - count(DISTINCT {', '.join(refs)})")
        elif spec.kind == "freshness":
            column = spec.columns[0]
            ref = _ref(column, types[column])
            if types[column].startswith("timestamp[") and "," in types[column]:
                ref = f"unix_micros({ref})"
            out.append(f"max({ref})")
    return out


def violation_columns(df: Any, specs: Sequence[ExpectationSpec], schema: Schema) -> Any:
    """`df` plus `__pipeloom_v<i>`, one non-null boolean column per row check of `specs`.

    An `expr` check sees float columns with NaN read as null; the original values are put back
    afterwards, so kept and quarantined rows are unchanged.
    """
    from pyspark.sql import functions as F

    rows = row_checks(specs)
    floats = [c.name for c in schema if c.dtype.startswith("float")]
    clean = floats if any(s.kind == "expr" for s in rows) else []
    raw = {f"{_RAW}{i}": F.col(quote_ident(n)) for i, n in enumerate(clean)}
    if clean:
        df = df.withColumns(raw).withColumns({n: F.expr(_ref(n, "float")) for n in clean})
    df = df.withColumns(
        {violation_name(i): F.expr(violation_sql(s, schema)) for i, s in enumerate(rows)}
    )
    if clean:
        df = df.withColumns({n: F.col(r) for n, r in zip(clean, raw, strict=True)}).drop(*raw)
    return df


def evaluate(rel: Relation, checks: Sequence[ExpectationSpec], schema: Schema) -> QualityCounts:
    from pyspark.sql import functions as F

    rows = row_checks(checks)
    marked = violation_columns(rel.native, rows, schema) if rows else rel.native
    values = list(marked.agg(*(F.expr(e) for e in aggregate_sql(checks, schema))).first())
    total = int(values.pop(0))
    failed = {s.name: int(values.pop(0)) for s in rows}
    dropped = int(values.pop(0)) if any(s.action == "drop" for s in rows) else 0
    types = _dtypes(schema)
    aggregates: dict[str, Any] = {}
    for spec in checks:
        if spec.kind in ("row_count", "unique"):
            aggregates[spec.name] = int(values.pop(0))
        elif spec.kind == "freshness":
            value = values.pop(0)
            dtype = types[spec.columns[0]]
            if value is not None and dtype.startswith("timestamp[") and "," in dtype:
                value = _EPOCH + timedelta(microseconds=value)
            aggregates[spec.name] = value
    return QualityCounts(total, failed, dropped, aggregates)


def _micros(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    delta = ts - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds


def failed_sql(names: Sequence[str]) -> str:
    """`_pipeloom_failed`: the names of the drop checks a row violates, in declaration order."""
    items = ", ".join(
        f"CASE WHEN {violation_name(i)} THEN {literal(n, 'string')} END"
        for i, n in enumerate(names)
    )
    return f"array_compact(array({items}))"


def apply_drops(
    rel: Relation,
    checks: Sequence[ExpectationSpec],
    schema: Schema,
    quarantine: QuarantineMeta | None,
) -> tuple[Any, Any | None]:
    """(kept, quarantined) native frames; `quarantined` is None without `quarantine`."""
    from pyspark.sql import functions as F

    drops = [c for c in row_checks(checks) if c.action == "drop"]
    original = [F.col(quote_ident(n)) for n in rel.native.columns]
    if not drops:
        kept = rel.native
        dropped = rel.native.limit(0)
        marked = None
    else:
        marked = violation_columns(rel.native, drops, schema)
        bad = F.expr(" OR ".join(violation_name(i) for i in range(len(drops))))
        kept = marked.where(~bad).select(*original)
        dropped = marked.where(bad)
    if quarantine is None:
        return kept, None
    failed = F.expr(failed_sql([d.name for d in drops])) if marked is not None else F.array()
    out = dropped.select(
        *original,
        F.lit(quarantine.run_id).alias(RUN_ID_COLUMN),
        failed.cast("array<string>").alias(FAILED_COLUMN),
        F.expr(f"timestamp_micros({_micros(quarantine.ts)})").alias(TS_COLUMN),
    )
    return kept, out
