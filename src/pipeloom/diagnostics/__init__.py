"""Diagnostics: the one way every layer reports a finding.

This is a leaf package. It imports nothing else from pipeloom, so every other layer may use it.
"""

from __future__ import annotations

import difflib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Generic, TypeAlias, TypeVar

JsonValue: TypeAlias = str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]

T = TypeVar("T")

__all__ = [
    "Diagnostic",
    "JsonValue",
    "Location",
    "Outcome",
    "Report",
    "Severity",
    "format_diagnostic",
    "suggest",
]


class Severity(StrEnum):
    """How serious a diagnostic is."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass(frozen=True, slots=True)
class Location:
    """Where a finding points. Paths are POSIX and relative to the project dir; lines are 1-based."""

    file: str | None = None
    line: int | None = None
    col: int | None = None
    json_pointer: str | None = None

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "file": self.file,
            "line": self.line,
            "col": self.col,
            "json_pointer": self.json_pointer,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Location:
        return cls(data.get("file"), data.get("line"), data.get("col"), data.get("json_pointer"))


@dataclass(frozen=True, slots=True)
class Diagnostic:
    """One finding with a stable code from the catalog."""

    code: str
    severity: Severity
    message: str
    hint: str | None = None
    location: Location | None = None
    context: Mapping[str, JsonValue] = field(default_factory=dict)

    def to_json(self) -> dict[str, JsonValue]:
        return {
            "code": self.code,
            "severity": self.severity.value,
            "message": self.message,
            "hint": self.hint,
            "location": self.location.to_json() if self.location else None,
            "context": dict(self.context),
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> Diagnostic:
        loc = data.get("location")
        return cls(
            code=data["code"],
            severity=Severity(data["severity"]),
            message=data["message"],
            hint=data.get("hint"),
            location=Location.from_json(loc) if loc else None,
            context=dict(data.get("context") or {}),
        )


def _sort_key(d: Diagnostic) -> tuple[str, int, int, str, str]:
    loc = d.location
    return (
        (loc.file or "") if loc else "",
        (loc.line or 0) if loc else 0,
        (loc.col or 0) if loc else 0,
        d.code,
        d.message,
    )


class Report:
    """Collects diagnostics during one command."""

    def __init__(self) -> None:
        self._items: list[Diagnostic] = []

    def add(
        self,
        code: str,
        message: str,
        *,
        location: Location | None = None,
        hint: str | None = None,
        context: Mapping[str, JsonValue] | None = None,
        severity: Severity | None = None,
    ) -> Diagnostic:
        """Record a finding. The severity defaults to the catalog's."""
        from pipeloom.diagnostics.codes import info

        diagnostic = Diagnostic(
            code=code,
            severity=severity if severity is not None else info(code).severity,
            message=message,
            hint=hint,
            location=location,
            context=dict(context or {}),
        )
        self._items.append(diagnostic)
        return diagnostic

    def extend(self, diagnostics: Iterable[Diagnostic]) -> None:
        """Add diagnostics produced elsewhere."""
        self._items.extend(diagnostics)

    @property
    def has_errors(self) -> bool:
        return self.error_count > 0

    @property
    def error_count(self) -> int:
        return sum(1 for d in self._items if d.severity is Severity.ERROR)

    def sorted(self) -> tuple[Diagnostic, ...]:
        return tuple(sorted(self._items, key=_sort_key))


@dataclass(frozen=True)
class Outcome(Generic[T]):
    """A value (or None on failure) together with everything found while producing it."""

    value: T | None
    diagnostics: tuple[Diagnostic, ...]


def suggest(word: str, candidates: Iterable[str]) -> str | None:
    """Return a did-you-mean sentence for the closest candidate, or None."""
    close = difflib.get_close_matches(word, list(candidates), n=1, cutoff=0.6)
    return f"did you mean '{close[0]}'?" if close else None


def format_diagnostic(d: Diagnostic) -> str:
    """Render one diagnostic as text, for example `error PLR201 models/m.yml:12:14 message`."""
    parts = [d.severity.value, d.code]
    loc = d.location
    if loc is not None and loc.file is not None:
        text = loc.file
        if loc.line is not None:
            text += f":{loc.line}"
            if loc.col is not None:
                text += f":{loc.col}"
        parts.append(text)
    parts.append(d.message)
    out = " ".join(parts)
    if d.hint:
        out += f"\n      hint: {d.hint}"
    return out
