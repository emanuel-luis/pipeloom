"""SQL text helpers and the mapping between logical types and DuckDB types."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Final

from pipeloom.engine.types import Column, Schema


def ident(name: str) -> str:
    """A quoted identifier."""
    return '"' + name.replace('"', '""') + '"'


def string(value: str) -> str:
    """A quoted string literal."""
    return "'" + value.replace("'", "''") + "'"


def literal(value: Any) -> str:
    """A SQL literal for a JSON or plain Python value."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float | Decimal):
        return repr(float(value)) if isinstance(value, float) else str(value)
    if isinstance(value, str):
        return string(value)
    if isinstance(value, datetime):
        kind = "TIMESTAMPTZ" if value.tzinfo is not None else "TIMESTAMP"
        return f"CAST({string(value.isoformat())} AS {kind})"
    if isinstance(value, date | time):
        return f"CAST({string(value.isoformat())} AS {type(value).__name__.upper()})"
    if isinstance(value, Sequence):
        return "[" + ", ".join(literal(v) for v in value) + "]"
    if isinstance(value, Mapping):
        return "{" + ", ".join(f"{string(str(k))}: {literal(v)}" for k, v in value.items()) + "}"
    raise TypeError(f"no SQL literal for {type(value).__name__}: {json.dumps(str(value))}")


def named_args(options: Mapping[str, Any]) -> str:
    """`, name = value` for each option of a table function such as read_csv."""
    return "".join(f", {k} = {literal(v)}" for k, v in options.items())


_SIMPLE: Final = {
    "TINYINT": "int8",
    "SMALLINT": "int16",
    "INTEGER": "int32",
    "BIGINT": "int64",
    "HUGEINT": "int128",
    "UTINYINT": "uint8",
    "USMALLINT": "uint16",
    "UINTEGER": "uint32",
    "UBIGINT": "uint64",
    "UHUGEINT": "uint128",
    "FLOAT": "float32",
    "DOUBLE": "float64",
    "VARCHAR": "string",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIME": "time",
    "BLOB": "binary",
    "TIMESTAMP": "timestamp[us]",
    "TIMESTAMP_MS": "timestamp[ms]",
    "TIMESTAMP_NS": "timestamp[ns]",
    "TIMESTAMP WITH TIME ZONE": "timestamp[us, UTC]",
    "INTERVAL": "duration[us]",
    '"NULL"': "null",
    "NULL": "null",
}
_TO_DUCKDB: Final = {v: k for k, v in _SIMPLE.items() if k not in ('"NULL"', "NULL")}
_DECIMAL = re.compile(r"DECIMAL\((\d+),\s*(\d+)\)")
_LOGICAL_DECIMAL = re.compile(r"decimal\((\d+),(\d+)\)")
_LOGICAL_TS = re.compile(r"timestamp\[(ms|us|ns)(?:, ([^\]]+))?\]")


def _split(body: str) -> list[str]:
    """Split on top-level commas."""
    parts, depth, cur = [], 0, ""
    for ch in body:
        if ch in "(<[":
            depth += 1
        elif ch in ")>]":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    return parts


def to_logical(duck: str) -> str:
    """The logical type of a DuckDB type name, or "unknown"."""
    if duck in _SIMPLE:
        return _SIMPLE[duck]
    if duck.startswith("ENUM("):
        return "string"
    if duck.endswith("[]"):
        return f"list<{to_logical(duck[:-2])}>"
    dec = _DECIMAL.fullmatch(duck)
    if dec:
        return f"decimal({dec.group(1)},{dec.group(2)})"
    if duck.startswith("STRUCT(") and duck.endswith(")"):
        fields = []
        for part in _split(duck[7:-1]):
            if part.startswith('"'):
                end = part.index('"', 1)
                name, rest = part[1:end], part[end + 1 :]
            else:
                name, _, rest = part.partition(" ")
            fields.append(f"{name}:{to_logical(rest.strip())}")
        return "struct<" + ",".join(fields) + ">"
    return "unknown"


def to_duckdb(logical: str) -> str | None:
    """The DuckDB type of a logical type, or None for one this engine cannot name. A timestamp
    in any zone is a TIMESTAMPTZ: DuckDB stores the instant, not the zone."""
    simple = _TO_DUCKDB.get(logical)
    if simple is not None:
        return simple
    dec = _LOGICAL_DECIMAL.fullmatch(logical)
    if dec:
        return f"DECIMAL({dec.group(1)},{dec.group(2)})"
    ts = _LOGICAL_TS.fullmatch(logical)
    if ts:
        return "TIMESTAMP WITH TIME ZONE" if ts.group(2) else _TO_DUCKDB[f"timestamp[{ts.group(1)}]"]
    if logical.startswith("list<") and logical.endswith(">"):
        inner = to_duckdb(logical[5:-1])
        return None if inner is None else f"{inner}[]"
    if logical.startswith("struct<") and logical.endswith(">"):
        fields = []
        for part in _split(logical[7:-1]):
            name, _, dtype = part.partition(":")
            inner = to_duckdb(dtype)
            if inner is None:
                return None
            fields.append(f"{ident(name)} {inner}")
        return "STRUCT(" + ", ".join(fields) + ")"
    return None


def schema_of(columns: Sequence[str], types: Sequence[Any]) -> Schema:
    return tuple(Column(n, to_logical(str(t))) for n, t in zip(columns, types, strict=True))


def is_float(duck: str) -> bool:
    return duck in ("FLOAT", "DOUBLE")
