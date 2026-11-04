"""The manifest (`pipeloom.manifest/1`): the plan as a JSON file, its hash and its checksums."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Final

from pydantic import ValidationError

from pipeloom.config.models import ResolvedModelConfig
from pipeloom.diagnostics import Location, Report
from pipeloom.plan.model import Plan

MANIFEST_SCHEMA: Final = "pipeloom.manifest/1"
_HASHED_METADATA = ("project", "target", "boundary", "config_version")


def canonical_json(value: Any) -> bytes:
    """Sorted keys, no spaces, no NaN, UTF-8: the bytes every hash is computed over."""
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False, ensure_ascii=False
    ).encode("utf-8")


def node_checksum(config: ResolvedModelConfig, body: bytes) -> str:
    """sha256 of the canonical config plus the body bytes, with CRLF read as LF."""
    digest = hashlib.sha256(canonical_json(config.model_dump(mode="json", by_alias=True)))
    digest.update(b"\0")
    digest.update(body.replace(b"\r\n", b"\n"))
    return digest.hexdigest()


def _sections(plan: Plan) -> dict[str, Any]:
    """The parts of the manifest that describe the pipeline, in manifest order."""
    return {
        "nodes": {
            uid: n.model_dump(mode="json", by_alias=True) for uid, n in sorted(plan.nodes.items())
        },
        "sources": {uid: s.model_dump(mode="json") for uid, s in sorted(plan.sources.items())},
        "units": {u: list(unit.nodes) for u, unit in sorted(plan.units.items())},
        "unit_parent_map": {u: list(unit.parents) for u, unit in sorted(plan.units.items())},
        "parent_map": {k: list(v) for k, v in sorted(plan.parent_map.items())},
        "child_map": {k: list(v) for k, v in sorted(plan.child_map.items())},
    }


def compute_plan_hash(plan: Plan) -> str:
    """sha256 of the canonical plan: nodes, sources, units, edges, project, target, boundary and
    config version. Selection, versions, timestamps and ids are left out."""
    meta = plan.metadata.model_dump(mode="json")
    doc = {**_sections(plan), "metadata": {k: meta[k] for k in _HASHED_METADATA}}
    return hashlib.sha256(canonical_json(doc)).hexdigest()


def manifest_dict(plan: Plan) -> dict[str, Any]:
    sections = _sections(plan)
    return {
        "schema": MANIFEST_SCHEMA,
        "metadata": plan.metadata.model_dump(mode="json"),
        **sections,
        "selectors": plan.selection.model_dump(mode="json"),
    }


def manifest_bytes(plan: Plan) -> bytes:
    """Indented by 2, UTF-8, LF, with a trailing newline."""
    text = json.dumps(manifest_dict(plan), indent=2, allow_nan=False, ensure_ascii=False)
    return (text + "\n").encode("utf-8")


def write_manifest(plan: Plan, path: Path) -> bytes:
    """Write the manifest, creating parent folders. Returns the bytes written."""
    content = manifest_bytes(plan)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return content


def read_manifest(path: Path, report: Report) -> Plan | None:
    """Read a manifest back into a Plan. None after reporting PLX107."""

    def fail(why: str) -> None:
        report.add(
            "PLX107",
            f"manifest is not usable: {why}",
            location=Location(path.as_posix()),
            hint="compile again to write a fresh manifest",
        )

    try:
        data = json.loads(path.read_bytes().decode("utf-8"))
    except (OSError, ValueError) as exc:  # JSONDecodeError and UnicodeDecodeError are ValueErrors
        fail(f"{type(exc).__name__}: {exc}")
        return None
    if not isinstance(data, dict) or data.get("schema") != MANIFEST_SCHEMA:
        fail(f"expected schema '{MANIFEST_SCHEMA}'")
        return None
    try:
        units = {
            uid: {"unit_id": uid, "nodes": nodes, "parents": data["unit_parent_map"][uid]}
            for uid, nodes in data["units"].items()
        }
        plan = Plan.model_validate(
            {
                "metadata": data["metadata"],
                "nodes": data["nodes"],
                "sources": data["sources"],
                "units": units,
                "parent_map": data["parent_map"],
                "child_map": data["child_map"],
                "selection": data["selectors"],
            }
        )
    except (ValidationError, KeyError, TypeError, AttributeError) as exc:
        fail(f"{type(exc).__name__}: {str(exc).splitlines()[0]}")
        return None
    if compute_plan_hash(plan) != plan.metadata.plan_hash:
        fail("plan_hash does not match the content; the file was edited")
        return None
    return plan
