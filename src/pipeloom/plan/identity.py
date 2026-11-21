"""Canonical dataset identity (ADR-0055): the one place that names the datasets a plan reads and
writes. The Airflow assets, the unit graph, the OpenLineage sink and the Dagster plugin all call
it. Pure: no disk, no plugin, no engine.

The rule is frozen at 1.0.0. A name 1.0 produced never changes. The only names changed are the
ones 1.0 could not use: an asset URI above the 1500 characters Airflow accepts ends in `_` and 8
hex of the sha256 of the full URI.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Final
from urllib.parse import quote, urlsplit

from pipeloom.plan.model import Plan

DEFAULT_URI_TEMPLATE: Final = "pipeloom://{project}/{target}/{relation}"
URI_PLACEHOLDERS: Final = ("project", "target", "model", "relation", "unique_id")
URI_MAX: Final = 1500
"""Airflow rejects an asset URI or name longer than this."""


def relation_name(catalog: str | None, schema: str, name: str) -> str:
    """`catalog.schema.name`, leaving out an empty catalog."""
    return ".".join(p for p in (catalog, schema, name) if p)


def asset_uri(
    template: str, *, project: str, target: str, model: str, relation: str, unique_id: str
) -> str:
    """Fill the placeholders; each value is percent-encoded (`._-~` kept) so the URI stays one
    string. A result above URI_MAX is cut and ends in `_` and 8 hex of the sha256 of the whole."""
    values = {
        "project": project,
        "target": target,
        "model": model,
        "relation": relation,
        "unique_id": unique_id,
    }
    uri = template.format_map({k: quote(v, safe="._-~") for k, v in values.items()})
    if len(uri) <= URI_MAX:
        return uri
    return f"{uri[: URI_MAX - 9]}_{hashlib.sha256(uri.encode('utf-8')).hexdigest()[:8]}"


def default_asset_uri(project: str, target: str, relation: str) -> str:
    """The URI of a persisted relation under the default template."""
    return asset_uri(
        DEFAULT_URI_TEMPLATE,
        project=project,
        target=target,
        model="",
        relation=relation,
        unique_id="",
    )


def relation_dataset(
    namespace: str, catalog: str | None, schema: str, name: str
) -> tuple[str, str]:
    """OpenLineage (namespace, name) of a model's relation."""
    return namespace, relation_name(catalog, schema, name)


def location_dataset(location: str, project_dir: Path | None = None) -> tuple[str, str]:
    """OpenLineage naming for a path (https://openlineage.io/docs/spec/naming): a URI with a
    host is `scheme://host` and the path without its leading slash; `file:` is namespace `file`
    and the absolute path; another scheme without a host is the scheme and the path as reported;
    a relative path is joined to the project dir when one is given."""
    parts = urlsplit(location)
    # a Windows drive letter ("C:/x") parses as a one-letter scheme: treat it as a path
    if len(parts.scheme) > 1:
        if parts.scheme == "file":
            return "file", parts.path
        if parts.netloc:
            return f"{parts.scheme}://{parts.netloc}", parts.path.lstrip("/")
        return parts.scheme, parts.path
    path = PurePosixPath(location)
    if not path.is_absolute() and project_dir is not None:
        return "file", (project_dir / location).as_posix()
    return "file", path.as_posix()


def source_dataset(
    namespace: str, *, table: str | None, location: str | None, project_dir: Path | None = None
) -> tuple[str, str] | None:
    """OpenLineage (namespace, name) of a source: its catalog table in `namespace`, else its
    location, else None."""
    if table:
        return namespace, table
    if location:
        return location_dataset(location, project_dir)
    return None


def unit_inputs(nodes: Sequence[str], inputs: Mapping[str, Mapping[str, str]]) -> list[str]:
    """Unique ids a unit reads, in first-seen order. `nodes` are the unit's members (inlined
    ephemeral models first) and `inputs` maps a unique id to its alias-to-unique-id inputs, as in
    a plan or manifest. A member's input that is another member is not an input, so an ephemeral
    input resolves to the persisted models and sources behind it."""
    inside = set(nodes)
    seen: dict[str, None] = {}
    for uid in nodes:
        for parent in inputs.get(uid, {}).values():
            if parent not in inside:
                seen.setdefault(parent, None)
    return list(seen)


def input_assets(plan: Plan, unit_id: str) -> tuple[str, ...]:
    """Unique ids of the persisted models and sources a unit reads. An ephemeral input is
    resolved: the unit inlines it, so what it reads stands in its place."""
    members = plan.units[unit_id].nodes
    return tuple(unit_inputs(members, {u: plan.nodes[u].inputs for u in members}))


def output_assets(plan: Plan, unit_id: str) -> tuple[str, ...]:
    """The unique id the unit writes, or none when the persisted model has no relation."""
    last = plan.units[unit_id].nodes[-1]
    return (last,) if plan.nodes[last].relation is not None else ()
