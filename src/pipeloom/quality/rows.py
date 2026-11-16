"""Rows as plain Python values: type inference, casts, CSV reading and the multiset diff that
unit tests use (ADR-0045). `pipeloom.testing.reference` re-exports it."""

from __future__ import annotations

import csv
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

Row = dict[str, Any]


def _null(value: Any) -> bool:
    return value is None or (isinstance(value, float) and value != value)  # noqa: PLR0124


def _parses(value: str, kind: type) -> bool:
    try:
        kind(value)
    except ValueError:
        return False
    return True


def infer_dtype(values: Sequence[Any], *, from_csv: bool = False) -> str | None:
    """The logical type of a column's values; None when every value is null.

    CSV values are strings: int64 when all parse as integers, float64 when all parse as floats,
    else string. Inline values take the type of their Python value; ints mixed with floats are
    float64."""
    present = [v for v in values if not _null(v)]
    if not present:
        return None
    if from_csv:
        if all(_parses(v, int) for v in present):
            return "int64"
        if all(_parses(v, float) for v in present):
            return "float64"
        return "string"
    kinds = set()
    for v in present:
        if isinstance(v, bool):
            kinds.add("boolean")
        elif isinstance(v, int):
            kinds.add("int64")
        elif isinstance(v, float):
            kinds.add("float64")
        elif isinstance(v, datetime):
            kinds.add("timestamp[us]" if v.tzinfo is None else "timestamp[us, UTC]")
        elif isinstance(v, date):
            kinds.add("date")
        elif isinstance(v, Decimal):
            kinds.add("float64")
        else:
            kinds.add("string")
    if kinds == {"int64", "float64"}:
        return "float64"
    if len(kinds) == 1:
        return kinds.pop()
    return "string"


def coerce(value: Any, dtype: str) -> Any:
    """`value` as the Python value of logical type `dtype`; null stays None. Raises ValueError
    (or TypeError) when it does not cast."""
    if _null(value):
        return None
    if dtype.startswith(("int", "uint")):
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"{value!r} is not an integer")
        return int(value)
    if dtype.startswith("float"):
        return float(value)
    if dtype == "boolean":
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in ("true", "1"):
            return True
        if text in ("false", "0"):
            return False
        raise ValueError(f"{value!r} is not a boolean")
    if dtype == "string":
        return str(value)
    if dtype.startswith("decimal"):
        return Decimal(str(value))
    if dtype == "date":
        if isinstance(value, datetime):
            return value.date()
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    if dtype.startswith("timestamp"):
        if isinstance(value, datetime):
            stamp = value
        elif isinstance(value, date):
            stamp = datetime(value.year, value.month, value.day)
        else:
            stamp = datetime.fromisoformat(str(value))
        if "," in dtype:  # timestamp[us, UTC]: aware; a naive value is read as UTC
            return stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)
        return stamp if stamp.tzinfo is None else stamp.astimezone(UTC).replace(tzinfo=None)
    return value


def read_csv_rows(path: Path) -> tuple[list[str], list[Row]]:
    """The header and the rows of a CSV file; an empty field is None, other values are strings."""
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        rows = [{k: (v if v != "" else None) for k, v in r.items()} for r in reader]
        return list(reader.fieldnames or ()), rows


def _norm(value: Any) -> Any:
    """A hashable form that compares the way unit tests compare."""
    if _null(value):
        return None
    if isinstance(value, float):
        return float(f"{value:.12g}")
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, list | tuple):
        return tuple(_norm(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _norm(v)) for k, v in value.items()))
    return value


@dataclass(frozen=True)
class RowDiff:
    missing: list[Row]
    """Expected rows the output lacks (one entry per missing copy)."""
    unexpected: list[Row]
    """Output rows that were not expected."""

    @property
    def ok(self) -> bool:
        return not self.missing and not self.unexpected


def diff_rows(
    expected: Sequence[Mapping[str, Any]],
    actual: Sequence[Mapping[str, Any]],
    columns: Sequence[str],
) -> RowDiff:
    """Compare `expected` with `actual` as multisets over `columns`: order is ignored, duplicates
    count, null equals NaN, floats compare to 12 significant digits, a naive timestamp is UTC."""

    def counted(
        rows: Sequence[Mapping[str, Any]],
    ) -> tuple[Counter[tuple[Any, ...]], dict[Any, Row]]:
        keys: Counter[tuple[Any, ...]] = Counter()
        first: dict[Any, Row] = {}
        for r in rows:
            key = tuple(_norm(r.get(c)) for c in columns)
            keys[key] += 1
            first.setdefault(key, {c: r.get(c) for c in columns})
        return keys, first

    want, want_rows = counted(expected)
    got, got_rows = counted(actual)
    missing = [want_rows[k] for k, n in (want - got).items() for _ in range(n)]
    unexpected = [got_rows[k] for k, n in (got - want).items() for _ in range(n)]
    return RowDiff(missing, unexpected)
