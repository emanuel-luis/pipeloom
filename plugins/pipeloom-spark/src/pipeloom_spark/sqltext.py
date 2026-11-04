"""SQL text helpers. Pure strings: nothing here imports Spark."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")
_INT = re.compile(r"u?int\d+")
_SCHEME = re.compile(r"[A-Za-z][A-Za-z0-9+.-]+:")


def has_scheme(path: str) -> bool:
    """True for a URI such as `s3://b/p`, `dbfs:/p` or `file:/p`. One letter before the colon is
    a Windows drive (C: then a path), which is a local path."""
    return _SCHEME.match(path) is not None


def quote_ident(name: str) -> str:
    """A backtick-quoted identifier; a backtick inside the name is doubled."""
    return "`" + name.replace("`", "``") + "`"


def wrap_inputs(query: str, aliases: Sequence[str]) -> tuple[str, dict[str, str]]:
    """The SQL to pass to `spark.sql(template, **frames)` and the keyword for each alias.

    Each alias becomes a CTE over a `{keyword}` field that Spark binds to a DataFrame, so an alias
    shadows a catalog table of the same name and no session temp view is created. Braces in the
    query are doubled, because Spark formats the text once it has keywords. Without aliases the
    query is returned as is, since Spark does not format a query that has no keywords.
    """
    text = query.strip().rstrip(";").rstrip()
    if not aliases:
        return text, {}
    kwargs = {alias: f"_pl_in{i}" for i, alias in enumerate(aliases)}
    ctes = ", ".join(
        f"{quote_ident(alias)} AS (SELECT * FROM {{{kw}}})" for alias, kw in kwargs.items()
    )
    body = text.replace("{", "{{").replace("}", "}}")
    return f"WITH {ctes} SELECT * FROM (\n{body}\n)", kwargs


def _micros(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    delta = value - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds


def _string(value: str) -> str:
    return f"decode(X'{value.encode('utf-8').hex()}','UTF-8')"


def _date(value: Any) -> str:
    if isinstance(value, datetime):
        value = value.date()
    return f"DATE '{value.isoformat() if isinstance(value, date) else value}'"


def _as_datetime(value: Any) -> Any:
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day)
    return value


def _timestamp_ntz(value: Any) -> str:
    value = _as_datetime(value)
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return f"TIMESTAMP_NTZ '{value.isoformat(sep=' ', timespec='microseconds')}'"


def _timestamp(value: Any) -> str:
    return f"timestamp_micros({_micros(_as_datetime(value))})"


def literal(value: Any, logical: str) -> str:
    """A SQL literal for a Python value of a logical type; `NULL` for None.

    Strings travel as hex so no quote or backslash can end the literal. Timestamps with a time
    zone are microseconds since the epoch, so the session time zone never matters.
    """
    if value is None:
        return "NULL"
    if logical.startswith("timestamp["):
        return _timestamp(value) if "," in logical else _timestamp_ntz(value)
    if logical == "date":
        return _date(value)
    dec = _DECIMAL.fullmatch(logical)
    if dec:
        return f"CAST('{value}' AS DECIMAL({dec.group(1)},{dec.group(2)}))"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int) and (_INT.fullmatch(logical) or logical in ("unknown", "")):
        return str(value)
    if logical in ("float32", "float64") or isinstance(value, float):
        kind = "FLOAT" if logical == "float32" else "DOUBLE"
        return f"CAST('{value!r}' AS {kind})"
    if isinstance(value, Decimal):
        return f"CAST('{value}' AS DECIMAL(38,18))"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, bytes):
        return f"X'{value.hex()}'"
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, date):
        return _date(value)
    return _string(str(value))


def partition_predicate(
    columns: Sequence[str], tuples: Sequence[tuple[Any, ...]], types: Sequence[str]
) -> str:
    """A `replaceWhere` predicate matching exactly the given partition tuples.

    One column: `c IN (...)` with `OR c IS NULL` for a null. Several: an OR of `(a <=> x AND ...)`
    terms, where `<=>` makes a null part match a null.
    """
    if not tuples:
        return "FALSE"
    if len(columns) == 1:
        col = quote_ident(columns[0])
        values = [t[0] for t in tuples if t[0] is not None]
        parts = [f"{col} IN ({', '.join(literal(v, types[0]) for v in values)})"] if values else []
        if len(values) != len(tuples):
            parts.append(f"{col} IS NULL")
        return " OR ".join(parts)
    terms = [
        "("
        + " AND ".join(
            f"{quote_ident(c)} <=> {literal(v, t)}"
            for c, v, t in zip(columns, tup, types, strict=True)
        )
        + ")"
        for tup in tuples
    ]
    return " OR ".join(terms)
