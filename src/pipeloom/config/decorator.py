"""Decorated Python models as the resolver sees them (ADR-0050 points 2, 7 and 11).

Discovery turns each `DiscoveredModel` into a `PythonModel`: its decorator arguments as plain,
validated YAML-shaped data, with `Var` markers resolved. `DecoratorSite` locates a JSON pointer
into those arguments with the interface of `LoadedDocument.locate`, so pydantic errors map
through `from_validation_error`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pipeloom.config.models import ColumnConfig, DataTestConfig, InputConfig
from pipeloom.diagnostics import Location

if TYPE_CHECKING:
    from pipeloom.discovery.model import Param

POINTER = "/@model"


@dataclass(frozen=True)
class DecoratorSite:
    """Where a `@model` decorator and each of its keyword arguments sit in a `.py` file."""

    file: str
    line: int
    col: int
    kwarg_locations: Mapping[str, tuple[int, int]] = field(default_factory=dict)
    data: Any = None
    """The plain decorator arguments, as `from_validation_error` walks them."""

    @property
    def path(self) -> str:
        return self.file

    def locate(self, pointer: str, *, key: bool = False) -> Location:
        """The keyword's own line and column when the pointer starts at one, else the decorator."""
        first = pointer.split("/")[1] if pointer.startswith("/") else ""
        first = first.replace("~1", "/").replace("~0", "~")
        line, col = self.kwarg_locations.get(first, (self.line, self.col))
        pointer = pointer.removeprefix(POINTER)
        return Location(self.file, line, col, POINTER + pointer)


@dataclass(frozen=True)
class PythonModel:
    """One decorated model with validated arguments."""

    name: str
    callable: str
    """`module:attr`."""
    module: str
    site: DecoratorSite
    settings: Mapping[str, Any]
    """Model settings by YAML key, plus `description` when set."""
    columns: list[ColumnConfig] | None
    data_tests: list[DataTestConfig] | None
    inputs: Mapping[str, InputConfig | str]
    """`inputs=` entries: a `ref`/`source` binding, or a bare name to resolve."""
    params: tuple[Param, ...]
    own_fields: frozenset[str]
