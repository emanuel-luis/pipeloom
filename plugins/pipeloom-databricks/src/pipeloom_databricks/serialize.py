"""Bytes for the rendered documents: the same input always gives the same bytes."""

from __future__ import annotations

import json
from collections.abc import Mapping

import yaml
from pydantic import JsonValue


def to_json_bytes(doc: Mapping[str, JsonValue]) -> bytes:
    """Indented by 2, ASCII only, LF, with a final newline. Keys keep their order."""
    return (json.dumps(doc, indent=2, ensure_ascii=True, allow_nan=False) + "\n").encode("ascii")


def to_bundle_yaml(resource_key: str, settings: Mapping[str, JsonValue], header: str) -> bytes:
    """A bundle file holding one job under `resources.jobs.<resource_key>`.

    `header` is comment text, one `#` line per line of it.
    """
    comment = "".join(f"# {line}".rstrip() + "\n" for line in header.splitlines())
    body = yaml.safe_dump(
        {"resources": {"jobs": {resource_key: dict(settings)}}},
        sort_keys=False,
        width=4096,
        allow_unicode=False,
        default_flow_style=False,
    )
    return (comment + body).encode("ascii")
