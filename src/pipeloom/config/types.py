"""Reusable field types for the configuration models."""

from __future__ import annotations

import re
from typing import Annotated, Any

from pydantic import AfterValidator, BeforeValidator, Field, StringConstraints, WithJsonSchema

IDENTIFIER_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"

Identifier = Annotated[str, StringConstraints(pattern=IDENTIFIER_PATTERN)]

_DURATION = re.compile(r"^(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")
_UNITS = (86_400, 3_600, 60, 1)


def parse_duration(value: Any) -> Any:
    """Turn "30m", "1h30m", "2d" or an int into seconds. Other values pass through."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
        match = _DURATION.match(text)
        if not text or match is None:
            raise ValueError(f"'{value}' is not a duration; use forms like 30m, 1h30m or 2d")
        return sum(int(g) * u for g, u in zip(match.groups(), _UNITS, strict=True) if g)
    return value


_DURATION_SCHEMA = {
    "anyOf": [
        {"type": "integer", "minimum": 0},
        {
            "type": "string",
            "pattern": r"^(?=.)(?:\d+d)?(?:\d+h)?(?:\d+m)?(?:\d+s)?$|^\d+$|^\$\{.+\}$",
        },
    ]
}

Duration = Annotated[
    int, BeforeValidator(parse_duration), Field(ge=0), WithJsonSchema(_DURATION_SCHEMA)
]
"""A duration in whole seconds; YAML may write it as 30m, 1h30m, 2d or an integer."""


def check_relative_posix(value: str) -> str:
    if value.startswith(("/", "\\")) or re.match(r"^[A-Za-z]:", value):
        raise ValueError("path must be relative to the project directory")
    if "\\" in value:
        raise ValueError("use forward slashes in paths")
    if ".." in value.split("/"):
        raise ValueError("path must not contain '..'")
    return value


RelativePosixPath = Annotated[str, AfterValidator(check_relative_posix)]
