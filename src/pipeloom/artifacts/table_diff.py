"""`pipeloom diff` (ADR-0053): a keyed comparison of one model's relation in a reference and in
this target, as a `pipeloom.table_diff/1` document and as markdown for a pull request comment.

The comparison runs on portable frames (`pipeloom.engine.portable.keyed_diff`); rows reach Python
only through `session.count` and `session.to_rows`, and only for the samples and one row of counts.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from datetime import date, datetime, time
from typing import Any, Final

from pydantic import BaseModel, ConfigDict

from pipeloom.engine.portable import DIFF_PREFIX, first_rows, keyed_diff
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import Relation

TABLE_DIFF_SCHEMA: Final = "pipeloom.table_diff/1"
SAMPLE_ROWS: Final = 10
"""Rows per sample list with `--samples`."""
MASK: Final = "***"
"""What a sample shows for a value of a column tagged `sensitive`."""


class DiffError(Exception):
    """The two relations cannot be compared, for example a key column is missing on one side."""


class _Closed(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Side(_Closed):
    relation: str
    """The relation name read on this side."""
    rows: int


class PerSide(_Closed):
    source: int
    target: int


class ColumnType(_Closed):
    name: str
    dtype: str


class TypeChange(_Closed):
    name: str
    source: str
    target: str


class SchemaDiff(_Closed):
    """Columns by name, case-sensitive. Order and nullability are ignored."""

    added: tuple[ColumnType, ...]
    """In the target only."""
    removed: tuple[ColumnType, ...]
    """In the source only."""
    type_changed: tuple[TypeChange, ...]
    """On both sides with another logical type; their values are not compared."""


class Counts(_Closed):
    matched: int
    """Rows whose key is on both sides with equal values in every compared column."""
    partial: int
    """Rows whose key is on both sides with a difference in at least one compared column."""
    source_only: int
    target_only: int
    duplicate_keys: PerSide
    """Keys that occur more than once on a side. Their rows are left out of the other counts."""
    null_keys: PerSide
    """Rows whose key has a null. They are left out of the other counts."""


class ColumnDiff(_Closed):
    name: str
    differs: int
    """Matched keys whose value in this column differs."""


class Samples(_Closed):
    """Up to `SAMPLE_ROWS` rows each, in key order. Columns tagged `sensitive` read `***`."""

    source_only: tuple[dict[str, Any], ...]
    target_only: tuple[dict[str, Any], ...]
    partial: tuple[dict[str, Any], ...]
    """`key` (the key columns) and `changes` (column to `source` and `target` values)."""


class TableDiff(_Closed):
    """The `pipeloom.table_diff/1` document, without its `schema` tag (`table_diff_dict` adds it)."""

    model: str
    key: tuple[str, ...]
    source: Side
    """The reference relation (`--state`)."""
    target: Side
    """The relation of this target."""
    differences: bool
    """True when the schemas or any row differ, or a key is duplicated or null."""
    schema_diff: SchemaDiff
    counts: Counts
    columns: tuple[ColumnDiff, ...]
    """The compared columns: on both sides with the same type, key columns excluded."""
    samples: Samples | None = None
    """Present with `--samples` only."""


def table_diff_dict(diff: TableDiff) -> dict[str, Any]:
    return {"schema": TABLE_DIFF_SCHEMA, **diff.model_dump(mode="json", exclude_none=True)}


def _plain(value: Any) -> Any:
    """A JSON value: dates and times as ISO text, other non-JSON values as their text."""
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    return str(value)


def _shown(column: str, value: Any, sensitive: Collection[str]) -> Any:
    return MASK if column in sensitive else _plain(value)


def compare(
    session: EngineSession,
    source: Relation,
    target: Relation,
    *,
    model: str,
    names: tuple[str, str],
    key: Sequence[str],
    sensitive: Collection[str] = (),
    samples: bool = False,
) -> TableDiff:
    """Compare `source` (the reference) and `target` (this target) by `key`. `names` are their
    relation names. Raises DiffError when a key column is missing on a side or has
    another type on each side."""
    s_schema = {c.name: c.dtype for c in session.schema(source)}
    t_schema = {c.name: c.dtype for c in session.schema(target)}
    for side, cols in (("source", s_schema), ("target", t_schema)):
        missing = [k for k in key if k not in cols]
        if missing:
            raise DiffError(f"the {side} relation has no key column {', '.join(missing)}")
    schema_diff = SchemaDiff(
        added=tuple(ColumnType(name=n, dtype=d) for n, d in t_schema.items() if n not in s_schema),
        removed=tuple(
            ColumnType(name=n, dtype=d) for n, d in s_schema.items() if n not in t_schema
        ),
        type_changed=tuple(
            TypeChange(name=n, source=d, target=t_schema[n])
            for n, d in s_schema.items()
            if n in t_schema and t_schema[n] != d
        ),
    )
    retyped = [t for t in schema_diff.type_changed if t.name in key]
    if retyped:
        raise DiffError(
            "a key column has another type on each side: "
            + ", ".join(
                f"{t.name} is {t.source} in the source and {t.target} in the target"
                for t in retyped
            )
        )
    compared = [n for n, d in s_schema.items() if n not in key and t_schema.get(n) == d]
    frames = keyed_diff(source.native, target.native, key, compared)

    def rel(native: Any) -> Relation:
        return Relation(native, source.engine, source.lazy)

    def count(native: Any) -> int:
        return session.count(rel(native))

    def first(native: Any) -> list[dict[str, Any]]:
        return session.to_rows(rel(first_rows(native, key, SAMPLE_ROWS)))

    (stats,) = session.to_rows(rel(frames.stats))
    per_column = [int(stats.get(f"{DIFF_PREFIX}d{i}") or 0) for i in range(len(compared))]
    joined, partial = int(stats["rows"] or 0), int(stats["partial"] or 0)
    counts = Counts(
        matched=joined - partial,
        partial=partial,
        source_only=count(frames.source_only),
        target_only=count(frames.target_only),
        duplicate_keys=PerSide(
            source=count(frames.duplicates[0]), target=count(frames.duplicates[1])
        ),
        null_keys=PerSide(source=count(frames.null_keys[0]), target=count(frames.null_keys[1])),
    )
    found: Samples | None = None
    if samples:
        found = Samples(
            source_only=tuple(
                {c: _shown(c, v, sensitive) for c, v in r.items()}
                for r in first(frames.source_only)
            ),
            target_only=tuple(
                {c: _shown(c, v, sensitive) for c, v in r.items()}
                for r in first(frames.target_only)
            ),
            partial=tuple(
                {
                    "key": {k: _shown(k, r[k], sensitive) for k in key},
                    "changes": {
                        c: {
                            "source": _shown(c, r[f"{DIFF_PREFIX}s{i}"], sensitive),
                            "target": _shown(c, r[f"{DIFF_PREFIX}t{i}"], sensitive),
                        }
                        for i, c in enumerate(compared)
                        if r[f"{DIFF_PREFIX}d{i}"]
                    },
                }
                for r in first(frames.partial)
            ),
        )
    differences = bool(
        schema_diff.added
        or schema_diff.removed
        or schema_diff.type_changed
        or counts.partial
        or counts.source_only
        or counts.target_only
        or counts.duplicate_keys.source
        or counts.duplicate_keys.target
        or counts.null_keys.source
        or counts.null_keys.target
    )
    return TableDiff(
        model=model,
        key=tuple(key),
        source=Side(relation=names[0], rows=count(source.native)),
        target=Side(relation=names[1], rows=count(target.native)),
        differences=differences,
        schema_diff=schema_diff,
        counts=counts,
        columns=tuple(
            ColumnDiff(name=c, differs=n) for c, n in zip(compared, per_column, strict=True)
        ),
        samples=found,
    )


def _cell(value: Any) -> str:
    text = "null" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _table(header: Sequence[str], rows: Sequence[Sequence[Any]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(_cell(v) for v in row) + " |" for row in rows]
    return [*lines, ""]


def render_markdown(diff: TableDiff) -> str:
    """Markdown for a pull request comment. Without samples it holds counts, column names and
    types only, never a cell value."""
    c = diff.counts
    lines = [
        f"### Table diff: {diff.model}",
        "",
        (
            f"Source (reference) `{diff.source.relation}`, target `{diff.target.relation}`, "
            f"key {', '.join(f'`{k}`' for k in diff.key)}."
        ),
        "",
        *_table(
            ["", "Rows"],
            [
                ["Source rows", diff.source.rows],
                ["Target rows", diff.target.rows],
                ["Matched", c.matched],
                ["Partial", c.partial],
                ["Source only", c.source_only],
                ["Target only", c.target_only],
                [
                    "Duplicate keys (source / target)",
                    f"{c.duplicate_keys.source} / {c.duplicate_keys.target}",
                ],
                ["Null keys (source / target)", f"{c.null_keys.source} / {c.null_keys.target}"],
            ],
        ),
    ]
    s = diff.schema_diff
    changes = [
        *([col.name, "added", "", col.dtype] for col in s.added),
        *([col.name, "removed", col.dtype, ""] for col in s.removed),
        *([t.name, "type changed", t.source, t.target] for t in s.type_changed),
    ]
    if changes:
        lines += ["Schema changes:", "", *_table(["Column", "Change", "Source", "Target"], changes)]
    else:
        lines += ["No schema changes.", ""]
    differing = [[col.name, col.differs] for col in diff.columns if col.differs]
    if differing:
        lines += ["Columns that differ:", "", *_table(["Column", "Rows"], differing)]
    if diff.samples is not None:
        for title, rows in (
            ("Source only", diff.samples.source_only),
            ("Target only", diff.samples.target_only),
        ):
            if rows:
                header = list(rows[0])
                lines += [
                    f"{title} (first {len(rows)}):",
                    "",
                    *_table(header, [[r[h] for h in header] for r in rows]),
                ]
        if diff.samples.partial:
            body = [
                [
                    ", ".join(f"{k}={_cell(v)}" for k, v in r["key"].items()),
                    col,
                    change["source"],
                    change["target"],
                ]
                for r in diff.samples.partial
                for col, change in r["changes"].items()
            ]
            lines += [
                f"Partial (first {len(diff.samples.partial)}):",
                "",
                *_table(["Key", "Column", "Source", "Target"], body),
            ]
    return "\n".join(lines).rstrip("\n") + "\n"
