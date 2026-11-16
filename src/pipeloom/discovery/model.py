"""What discovery returns: the models it found and the files it could not load (ADR-0050 point 2).

Discovery returns data. Turning a failure into a located diagnostic with a code is the caller's job.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

FailureKind = Literal[
    "syntax",
    "import_error",
    "outside_roots",
    "name_mismatch",
    "unknown_keyword",
    "invalid_argument",
    "not_found",
]

ParamKind = Literal[
    "POSITIONAL_ONLY", "POSITIONAL_OR_KEYWORD", "VAR_POSITIONAL", "KEYWORD_ONLY", "VAR_KEYWORD"
]


@dataclass(frozen=True, slots=True)
class Param:
    """One parameter of a model function, in signature order."""

    name: str
    kind: ParamKind
    has_default: bool = False


@dataclass(frozen=True, slots=True)
class DiscoveredModel:
    """One function marked with `@model`.

    `file` is POSIX and relative to the project dir. `line` and `col` are 1-based: the `@model`
    decorator when it is written on the `def`, else the function's first line and column 1.
    `kwargs` holds the decorator arguments as `ModelSpec.kwargs` would; `Var` stays a marker.
    `kwarg_locations` maps an argument to its own (line, col) and is empty for a model built any
    other way than a literal decorator on the `def`.
    """

    name: str
    module: str
    attr: str
    file: str
    line: int
    col: int
    kwargs: Mapping[str, Any]
    kwarg_locations: Mapping[str, tuple[int, int]]
    params: tuple[Param, ...]
    dynamic: bool

    @property
    def callable_ref(self) -> str:
        """`module:attr`, as the manifest names it."""
        return f"{self.module}:{self.attr}"


@dataclass(frozen=True, slots=True)
class DiscoveryFailure:
    """A file, or one model in it, that discovery could not load. Lines and columns are 1-based."""

    file: str
    line: int | None
    col: int | None
    kind: FailureKind
    message: str
    traceback: str | None = None


@dataclass(frozen=True, slots=True)
class DiscoveryResult:
    models: tuple[DiscoveredModel, ...] = ()
    failures: tuple[DiscoveryFailure, ...] = field(default=())


def params_of(fn: Callable[..., Any]) -> tuple[Param, ...]:
    """The parameters of an imported function, in the shape the AST resolver produces."""
    return tuple(
        Param(p.name, p.kind.name, p.default is not inspect.Parameter.empty)  # type: ignore[arg-type]
        for p in inspect.signature(fn).parameters.values()
    )
