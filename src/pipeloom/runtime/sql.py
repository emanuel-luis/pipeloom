"""`ctx.sql` support: choose the dialect of a query and translate it to the engine's (ADR-0050)."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, NoReturn

from pipeloom.engine import portable
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import ReadSpec, Relation


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


class ViewInput:
    """An input parameter of a view model. Its rows are never read: the view names the input in
    `ctx.sql`, so any other use raises."""

    __slots__ = ("_alias",)

    def __init__(self, alias: str) -> None:
        self._alias = alias

    def _refuse(self, what: str) -> NoReturn:
        raise ViewModeError(
            f"used input '{self._alias}' as a frame ({what}); a view names it in ctx.sql only"
        )

    def __getattr__(self, name: str) -> NoReturn:
        self._refuse(f".{name}")

    def __getitem__(self, key: object) -> NoReturn:
        self._refuse("[...]")

    def __format__(self, spec: str) -> NoReturn:
        self._refuse("f-string")

    def __str__(self) -> NoReturn:  # noqa: PLE0307 - it always raises
        self._refuse("str()")

    def __repr__(self) -> str:
        return f"<view input '{self._alias}'>"

    def __iter__(self) -> NoReturn:
        self._refuse("iteration")

    def __len__(self) -> int:  # noqa: PLE0303 - it always raises
        self._refuse("len()")

    def __bool__(self) -> bool:  # noqa: PLE0304 - it always raises
        self._refuse("truth test")

    def __call__(self, *args: object, **kwargs: object) -> NoReturn:
        self._refuse("call")


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


def sql_runner(
    session: EngineSession,
    inputs: Mapping[str, Relation],
    *,
    node_dialect: str | None,
    engine_dialect: str | None,
    api: Literal["native", "narwhals"],
) -> Callable[[str, str | None, Mapping[str, Any] | None], Any]:
    """The `ModelContext._run_sql` callback: translate the query (the call's dialect, else the
    node's) to the engine's, run it over `inputs` plus the frames passed as `extra`, and return
    the result in the model's frame API."""

    def run_sql(query: str, dialect: str | None, extra: Mapping[str, Any] | None) -> Any:
        text = translate(query, dialect or node_dialect, engine_dialect)
        frames = dict(inputs)
        for name, frame in (extra or {}).items():
            frames[name] = session.relation(portable.from_frame_api(frame))
        return portable.to_frame_api(session.sql(text, frames).native, api)

    return run_sql
