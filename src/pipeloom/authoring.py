"""The Python authoring API: the `@model` decorator, input and variable helpers, check helpers and
short names for the config models a decorator takes.

`@model` only marks a function. It validates nothing and registers nothing: the arguments are
checked when the project is resolved, so a bad value becomes a located diagnostic instead of an
import error. The decorated function stays a plain function that tests can call.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Final, Literal, TypeVar, overload

from pipeloom.config.models import (
    AcceptedValuesCheck,
    CheckConfig,
    ColumnConfig,
    DeletesConfig,
    ExpectationConfig,
    FreshnessCheck,
    IncrementalConfig,
    QualityConfig,
    RangeCheck,
    RegexCheck,
    RowCountCheck,
    WriteConfig,
)
from pipeloom.runtime.context import ModelContext

__all__ = [
    "MARKER",
    "NO_DEFAULT",
    "Column",
    "Deletes",
    "Expectation",
    "Incremental",
    "ModelContext",
    "ModelSpec",
    "Quality",
    "Ref",
    "SourceRef",
    "Var",
    "Write",
    "accepted_values",
    "between",
    "freshness",
    "model",
    "not_null",
    "ref",
    "regex",
    "row_count",
    "source",
    "unique",
    "var",
]

Write = WriteConfig
Incremental = IncrementalConfig
Column = ColumnConfig
Expectation = ExpectationConfig
Quality = QualityConfig
Deletes = DeletesConfig

F = TypeVar("F", bound=Callable[..., Any])

MARKER: Final = "__pipeloom_model__"
"""Attribute `@model` sets on the function it marks."""


@dataclass(frozen=True, slots=True)
class ModelSpec:
    """The arguments given to `@model`, unvalidated. A key is present only when its argument was
    passed with a value other than None."""

    kwargs: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Ref:
    """An input that reads another model."""

    name: str


@dataclass(frozen=True, slots=True)
class SourceRef:
    """An input that reads a table of a source group."""

    group: str
    table: str


class _NoDefault(Enum):
    NO_DEFAULT = "NO_DEFAULT"


NO_DEFAULT: Final = _NoDefault.NO_DEFAULT
"""`Var.default` when `var` got no default."""


@dataclass(frozen=True, slots=True)
class Var:
    """A project variable, read when the project is resolved."""

    name: str
    default: Any = NO_DEFAULT


@overload
def model(fn: F, /) -> F: ...
@overload
def model(
    *,
    name: str | None = None,
    description: str | None = None,
    inputs: Mapping[str, str | Ref | SourceRef] | None = None,
    materialized: Literal["table", "incremental", "view", "ephemeral", "intermediate"]
    | None = None,
    incremental: Incremental | Mapping[str, Any] | None = None,
    write: Write | Mapping[str, Any] | None = None,
    partition_by: Sequence[str] | None = None,
    cluster_by: Sequence[str] | None = None,
    columns: Sequence[Column | Mapping[str, Any]] | None = None,
    expectations: Sequence[Expectation | Mapping[str, Any]] | None = None,
    quality: Quality | Mapping[str, Any] | None = None,
    tags: Sequence[str] | None = None,
    retries: int | None = None,
    retry_delay: str | float | None = None,
    retry_on: Literal["transient", "any"] | None = None,
    timeout: str | float | None = None,
    frame_api: Literal["native", "narwhals"] | None = None,
    sql_dialect: str | None = None,
    schema: str | None = None,
    catalog: str | None = None,
    meta: Mapping[str, Any] | None = None,
) -> Callable[[F], F]: ...
def model(fn: F | None = None, /, **kwargs: Any) -> F | Callable[[F], F]:
    """Mark a function as a pipeloom model, as `@model` or `@model(...)`. Returns the same
    function with a `ModelSpec` in `__pipeloom_model__`."""
    spec = ModelSpec(MappingProxyType({k: v for k, v in kwargs.items() if v is not None}))

    def mark(f: F) -> F:
        if hasattr(f, MARKER):
            raise TypeError(f"@model is applied twice to {getattr(f, '__qualname__', f)!r}")
        setattr(f, MARKER, spec)
        return f

    return mark if fn is None else mark(fn)


def ref(name: str) -> Ref:
    """Read the model `name`."""
    return Ref(name)


def source(group: str, table: str) -> SourceRef:
    """Read `table` of the source group `group`."""
    return SourceRef(group, table)


def var(name: str, default: Any = NO_DEFAULT) -> Var:
    """The project variable `name`, or `default` when it is not set."""
    return Var(name, default)


def not_null(*cols: str) -> CheckConfig:
    return CheckConfig(not_null=list(cols))


def unique(*cols: str) -> CheckConfig:
    return CheckConfig(unique=list(cols))


def accepted_values(col: str, values: Iterable[Any]) -> CheckConfig:
    return CheckConfig(accepted_values=AcceptedValuesCheck(column=col, values=list(values)))


def between(col: str, *, min: float | None = None, max: float | None = None) -> CheckConfig:
    """The `range` check."""
    return CheckConfig(range=RangeCheck(column=col, min=min, max=max))


def regex(col: str, pattern: str) -> CheckConfig:
    return CheckConfig(regex=RegexCheck(column=col, pattern=pattern))


def row_count(*, min: int | None = None, max: int | None = None) -> CheckConfig:
    return CheckConfig(row_count=RowCountCheck(min=min, max=max))


def freshness(col: str, max_age: str | int) -> CheckConfig:
    """`max_age` is seconds or a duration such as 30m, 1h30m or 2d."""
    check = FreshnessCheck.model_validate({"column": col, "max_age": max_age})
    return CheckConfig(freshness=check)
