"""`ctx.sql` support: choose the dialect of a query and translate it to the engine's (ADR-0050)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from pipeloom.engine.types import ReadSpec


class SqlUnsupportedError(Exception):
    """`ctx.sql` on an engine that runs no SQL (its `sql_dialect` is None)."""


class SqlglotMissingError(Exception):
    """A query needs translation and sqlglot is not installed."""


class ViewModeError(Exception):
    """A view model used an input or `ctx.sql` in a way a stored view cannot hold (PLX120)."""


@dataclass(frozen=True)
class ViewQuery:
    """What `ctx.sql` returns while a view model runs: the query, in the engine's dialect, and
    the relation each alias it names is bound to."""

    query: str
    inputs: Mapping[str, ReadSpec] = field(default_factory=dict)


def translate(query: str, dialect: str | None, engine: str | None) -> str:
    """`query`, written in `dialect`, in the engine's dialect. Translates only when a dialect is
    declared and differs from the engine's; sqlglot is imported only then."""
    if engine is None:
        raise SqlUnsupportedError("this engine has no SQL")
    if dialect is None or dialect == engine:
        return query
    try:
        import sqlglot
    except ImportError as exc:
        raise SqlglotMissingError(
            f"translating SQL from '{dialect}' to '{engine}' needs sqlglot; "
            "install pipeloom[sqlglot] or drop sql_dialect"
        ) from exc
    return ";\n".join(sqlglot.transpile(query, read=dialect, write=engine))
