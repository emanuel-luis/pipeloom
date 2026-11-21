"""JSON Schemas of the artifacts (ADR-0049): `manifest`, `run_results`, `event`, `unit_states`
and `unit_graph`.

Events come from the pydantic models in `pipeloom.events.model`, the unit graph from
`pipeloom.artifacts.unit_graph`, and the manifest's `$defs` from the plan models (its top level
is written here, as `manifest_dict` shapes it). Run results and unit states are written here by
hand, since their rows are plain dicts. The published schemas are open: a
consumer must ignore fields it does not know, so a later 1.x can add fields. `close()` builds the
strict variant the tests use to catch a field emitted by mistake.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, Final, get_args

from pydantic import BaseModel

from pipeloom.artifacts.results import RESULTS_SCHEMA
from pipeloom.artifacts.unit_graph import UNIT_GRAPH_SCHEMA, UnitGraph
from pipeloom.config.models import Materialized
from pipeloom.config.schema import DIALECT, SCHEMA_BASE_URL
from pipeloom.events.model import EVENT_SCHEMA, ErrorInfo, Event, EventPayload
from pipeloom.plan.manifest import MANIFEST_SCHEMA
from pipeloom.plan.model import PlanMetadata, PlanNode, PlanSelection, PlanSource

UNIT_STATES_SCHEMA: Final = "pipeloom.unit_states/1"
COMPILE_RESULT_SCHEMA: Final = "pipeloom.compile_result/1"

_DEFS: Final = "#/$defs/"
_STRING: Final = {"type": "string"}
_INTEGER: Final = {"type": "integer"}
_BOOLEAN: Final = {"type": "boolean"}
_STRINGS: Final = {"type": "array", "items": _STRING}
_OBJECT: Final = {"type": "object"}
# The resolved config keys every 1.0 manifest writes. Frozen: a field added later stays optional
# (ADR-0049), so it must not join this tuple.
MANIFEST_CONFIG_REQUIRED: Final = (
    "materialized",
    "write",
    "incremental",
    "partition_by",
    "cluster_by",
    "expectations",
    "quality",
    "tags",
    "retries",
    "retry_delay",
    "retry_on",
    "timeout",
    "frame_api",
    "schema",
    "catalog",
    "meta",
)
# The run_results keys every 1.0 document writes. Frozen like the manifest config keys: a field
# added later stays optional (ADR-0049), so it must not join these tuples.
ROW_REQUIRED: Final = (
    "unique_id",
    "unit_id",
    "ephemeral_inlined",
    "materialization",
    "strategy",
    "relation_name",
    "status",
    "attempt",
    "retries_used",
    "started_at",
    "ended_at",
    "duration_ms",
    "queue_wait_ms",
    "rows_read",
    "rows_written",
    "rows_inserted",
    "rows_updated",
    "rows_deleted",
    "rows_dropped",
    "rows_quarantined",
    "bytes_read",
    "bytes_written",
    "target_version",
    "full_refresh",
    "is_incremental",
    "watermark_before",
    "watermark_after",
    "schema_hash",
    "schema_changes",
    "expectations",
    "warnings",
    "error",
    "engine",
    "engine_version",
    "executor",
    "idempotency_token",
    "skip_reason",
    "orchestrator_ids",
)
RUN_REQUIRED: Final = (
    "run_id",
    "parent_run_id",
    "invocation_ids",
    "project",
    "target",
    "plan_hash",
    "selector",
    "full_refresh",
    "status",
    "started_at",
    "ended_at",
    "duration_ms",
    "counts",
    "totals",
    "identity",
    "pipeloom_version",
    "plugins",
    "python_version",
    "git_sha",
    "exit_code",
    "intermediate_ttl",
    "warnings",
)
METADATA_REQUIRED: Final = (
    "pipeloom_version",
    "generated_at",
    "invocation_id",
    "plugins",
)
_ORCHESTRATOR_IDS: Final = {
    "type": "object",
    "propertyNames": {"pattern": "^[a-z][a-z0-9_]{0,63}$"},
    "additionalProperties": {"type": "string", "minLength": 1, "maxLength": 256},
}


def _nullable(*types: str) -> dict[str, Any]:
    return {"type": [*types, "null"]}


def _open(node: Any) -> Any:
    """Drop the `additionalProperties: false` pydantic emits for `extra="forbid"`."""
    if isinstance(node, dict):
        return {
            k: _open(v) for k, v in node.items() if not (k == "additionalProperties" and v is False)
        }
    if isinstance(node, list):
        return [_open(v) for v in node]
    return node


def close(schema: Mapping[str, Any]) -> dict[str, Any]:
    """The strict variant: every object schema with `properties` gets `additionalProperties:
    false`. The `if` of a conditional is left as is and its `then` is not closed itself, since
    both describe a part of an object and not the whole of it."""

    def walk(node: Any, closable: bool = True) -> Any:
        if isinstance(node, list):
            return [walk(v) for v in node]
        if not isinstance(node, dict):
            return node
        out: dict[str, Any] = {}
        for key, value in node.items():
            if key == "if":
                out[key] = value
            elif key in ("then", "else"):
                out[key] = walk(value, closable=False)
            else:
                out[key] = walk(value)
        if closable and "properties" in out and "additionalProperties" not in out:
            out["additionalProperties"] = False
        return out

    closed: dict[str, Any] = walk(dict(schema))
    return closed


def _model(model: type[BaseModel], defs: dict[str, Any]) -> dict[str, Any]:
    """The model's schema with its nested definitions hoisted into `defs`."""
    schema = model.model_json_schema(ref_template=_DEFS + "{model}", mode="serialization")
    defs.update(schema.pop("$defs", {}))
    return schema


def _document(file: str, tag: str, title: str, body: dict[str, Any]) -> dict[str, Any]:
    """Every document names its schema and version in a required `schema` property."""
    props = dict(body.get("properties", {}))
    props["schema"] = {"const": tag, "description": "Schema name and version."}
    required = ["schema", *(r for r in body.get("required", []) if r != "schema")]
    doc: dict[str, Any] = {
        "$schema": DIALECT,
        "$id": SCHEMA_BASE_URL + file,
        "title": title,
        **body,
        "type": "object",
        "properties": props,
        "required": required,
    }
    out: dict[str, Any] = _open(doc)
    return out


def event_payloads() -> list[type[EventPayload]]:
    """The payloads the core emits. Subclasses defined elsewhere (tests, plugins) are left out,
    so the schema does not depend on what happens to be imported."""
    found = [c for c in EventPayload.__subclasses__() if c.__module__ == EventPayload.__module__]
    return sorted(found, key=lambda c: c.TYPE)


def event_schema() -> dict[str, Any]:
    """`pipeloom.event/1`: the envelope, and one `data` schema per known event type. A type not
    listed here keeps an open `data` object: event types are an open set."""
    defs: dict[str, Any] = {}
    envelope = _model(Event, defs)
    conditions = []
    for payload in event_payloads():
        defs[payload.__name__] = _model(payload, defs)
        conditions.append(
            {
                "if": {"properties": {"type": {"const": payload.TYPE}}, "required": ["type"]},
                "then": {"properties": {"data": {"$ref": _DEFS + payload.__name__}}},
            }
        )
    # Skip reasons are an open set, as in the run_results row `skip_reason`.
    defs["UnitSkipped"]["properties"]["reason"] = {
        "type": "string",
        "description": "upstream_failed, fail_fast, not_run, or a reason added later.",
    }
    body = {
        "description": "One event: the envelope, and a data object per event type.",
        "properties": envelope["properties"],
        "required": envelope.get("required", []),
        "allOf": conditions,
        "$defs": dict(sorted(defs.items())),
    }
    return _document("event.json", EVENT_SCHEMA, "pipeloom event", body)


def _row() -> dict[str, Any]:
    """One `results` row; the same object is an attempt file."""
    counter = _nullable("integer")
    props: dict[str, Any] = {
        "unique_id": {**_STRING, "description": "Model id, model.<project>.<name>."},
        "unit_id": {**_STRING, "description": "Execution unit id."},
        "ephemeral_inlined": {**_STRINGS, "description": "Ephemeral models inlined here."},
        "materialization": _nullable("string"),
        "strategy": _nullable("string"),
        "relation_name": _nullable("string"),
        "status": {"enum": ["succeeded", "failed", "skipped", "cancelled", "timed_out"]},
        "attempt": {**_nullable("integer"), "minimum": 1},
        "retries_used": {**_INTEGER, "minimum": 0},
        "started_at": {**_nullable("string"), "description": "RFC 3339, UTC."},
        "ended_at": {**_nullable("string"), "description": "RFC 3339, UTC."},
        "duration_ms": counter,
        "queue_wait_ms": counter,
        "rows_read": {
            "type": "object",
            "description": "Rows read per input alias; null where the engine is lazy.",
            "additionalProperties": counter,
        },
        "rows_written": counter,
        "rows_inserted": counter,
        "rows_updated": counter,
        "rows_deleted": counter,
        "rows_dropped": counter,
        "rows_quarantined": counter,
        "bytes_read": counter,
        "bytes_written": counter,
        "target_version": _nullable("integer", "string"),
        "full_refresh": _BOOLEAN,
        "is_incremental": _BOOLEAN,
        "watermark_before": {"description": "Any JSON value; ISO text for dates."},
        "watermark_after": {"description": "Any JSON value; ISO text for dates."},
        "schema_hash": _nullable("string"),
        "schema_changes": {"type": "array", "items": _OBJECT},
        "expectations": {"type": "array", "items": _OBJECT},
        "warnings": _STRINGS,
        "error": {"anyOf": [{"$ref": _DEFS + "error"}, {"type": "null"}]},
        "engine": _nullable("string"),
        "engine_version": _nullable("string"),
        "executor": _nullable("string"),
        "idempotency_token": _nullable("string"),
        "skip_reason": {
            **_nullable("string"),
            "description": "upstream_failed, fail_fast, not_run or an orchestrator's own reason.",
        },
        "orchestrator_ids": _ORCHESTRATOR_IDS,
    }
    return {"type": "object", "properties": props, "required": list(ROW_REQUIRED)}


def run_results_schema() -> dict[str, Any]:
    """`pipeloom.run_results/1`. `#/$defs/result` is also the schema of an attempt file."""
    defs: dict[str, Any] = {}
    error = _model(ErrorInfo, defs)
    defs["error"] = error
    defs["result"] = _row()
    run: dict[str, Any] = {
        "run_id": _STRING,
        "parent_run_id": _nullable("string"),
        "invocation_ids": _STRINGS,
        "project": _STRING,
        "target": _STRING,
        "plan_hash": _STRING,
        "selector": {
            "type": "object",
            "properties": {"select": _STRINGS, "exclude": _STRINGS},
            "required": ["select", "exclude"],
        },
        "full_refresh": _BOOLEAN,
        "status": {"enum": ["succeeded", "failed", "cancelled"]},
        "started_at": _STRING,
        "ended_at": _STRING,
        "duration_ms": _INTEGER,
        "counts": {"type": "object", "additionalProperties": _INTEGER},
        "totals": {"type": "object", "additionalProperties": _INTEGER},
        "identity": {
            "type": "object",
            "properties": {
                "user": _STRING,
                "host": _STRING,
                "orchestrator": _nullable("string"),
                "orchestrator_ids": _ORCHESTRATOR_IDS,
            },
            "required": ["user", "host", "orchestrator", "orchestrator_ids"],
        },
        "pipeloom_version": _STRING,
        "plugins": {"type": "object", "additionalProperties": _OBJECT},
        "python_version": _STRING,
        "git_sha": _nullable("string"),
        "exit_code": _INTEGER,
        "intermediate_ttl": {**_nullable("number"), "description": "Seconds."},
        "warnings": _STRINGS,
    }
    metadata = {
        "pipeloom_version": _STRING,
        "generated_at": _STRING,
        "invocation_id": _STRING,
        "plugins": {"type": "object", "additionalProperties": _OBJECT},
    }
    body = {
        "description": "The outcome of one run, one row per persisted model.",
        "properties": {
            "metadata": {
                "type": "object",
                "properties": metadata,
                "required": list(METADATA_REQUIRED),
            },
            "args": {**_OBJECT, "description": "The options of the invocation."},
            "run": {"type": "object", "properties": run, "required": list(RUN_REQUIRED)},
            "results": {"type": "array", "items": {"$ref": _DEFS + "result"}},
        },
        "required": ["metadata", "args", "run", "results"],
        "$defs": dict(sorted(defs.items())),
    }
    return _document("run_results.json", RESULTS_SCHEMA, "pipeloom run results", body)


def unit_states_schema() -> dict[str, Any]:
    """`pipeloom.unit_states/1`: what an orchestrator tells `pipeloom finalize` about units that
    wrote no attempt file."""
    state = {
        "type": "object",
        "properties": {
            "status": {"enum": ["skipped", "cancelled", "failed", "excluded"]},
            "reason": {
                **_nullable("string"),
                "description": "For skipped; null is upstream_failed.",
            },
            "code": {**_nullable("string"), "description": "For failed; null is PLX106."},
            "message": _nullable("string"),
            "orchestrator_ids": _ORCHESTRATOR_IDS,
        },
        "required": ["status"],
    }
    body = {
        "description": "Unit states known to the orchestrator, keyed by unit id.",
        "properties": {"units": {"type": "object", "additionalProperties": state}},
        "required": ["units"],
    }
    return _document("unit_states.json", UNIT_STATES_SCHEMA, "pipeloom unit states", body)


def compile_result_schema() -> dict[str, Any]:
    """`pipeloom.compile_result/1`: the output of `pipeloom compile --output json`."""
    props = {
        "project": _STRING,
        "target": _STRING,
        "plan_hash": _STRING,
        "manifest": {
            **_STRING,
            "description": "Manifest path, relative to the project dir unless absolute.",
        },
        "manifest_sha256": _STRING,
        "units": _INTEGER,
        "by_materialization": {"type": "object", "additionalProperties": _INTEGER},
        "ephemeral_inlined": _INTEGER,
        "selected": _INTEGER,
    }
    body = {
        "description": "What `compile` wrote: the manifest path and hash, and the plan counts.",
        "properties": props,
        "required": list(props),
    }
    return _document("compile_result.json", COMPILE_RESULT_SCHEMA, "pipeloom compile result", body)


def unit_graph_schema() -> dict[str, Any]:
    """`pipeloom.unit_graph/1`: the output of `pipeloom graph --units --output json`."""
    defs: dict[str, Any] = {}
    top = _model(UnitGraph, defs)
    body = {
        "description": "The execution units of a plan, their edges and their command lines.",
        "properties": top["properties"],
        "required": top.get("required", []),
        "$defs": dict(sorted(defs.items())),
    }
    return _document("unit_graph.json", UNIT_GRAPH_SCHEMA, "pipeloom unit graph", body)


def manifest_schema() -> dict[str, Any]:
    """`pipeloom.manifest/1`: the output of `pipeloom compile`. The top level is written here,
    as `manifest_dict` shapes it; the node, source and metadata objects come from the plan
    models, adjusted where `manifest_dict` reshapes their dump."""
    defs: dict[str, Any] = {}
    for model in (PlanMetadata, PlanNode, PlanSource, PlanSelection):
        defs[model.__name__] = _model(model, defs)
    node = defs["PlanNode"]["properties"]
    # The model types it as str; ADR-0049 makes it a closed enum.
    node["materialized"] = {"enum": list(get_args(Materialized))}
    # `_node` drops these keys while they are None, so they are an array or absent.
    for key in ("columns", "data_tests"):
        node[key] = node[key]["anyOf"][0]
    node["checksums"] = {
        "type": "object",
        "additionalProperties": _STRING,
        "description": "Split checksums (since 1.2): code, config, contract, tests and "
        "upstream. Absent in 1.0 manifests; not part of plan_hash.",
    }
    node["checksum_version"] = {
        **_INTEGER,
        "description": "How checksums was computed (since 1.2); absent with it.",
    }
    meta = defs["PlanMetadata"]["properties"]
    meta["deploy"] = {**_OBJECT, "description": "Absent when the target has no deploy block."}
    for key in ("generated_at", "invocation_id"):
        meta[key]["description"] = "An empty string in `compile --stable` output."
    config = defs["ResolvedModelConfig"]
    # `_config` drops `sql_dialect` while it is None.
    config["properties"]["sql_dialect"] = {**_STRING, "description": "Absent while unset."}
    config["required"] = list(MANIFEST_CONFIG_REQUIRED)
    ids = {"type": "object", "additionalProperties": _STRINGS}
    body = {
        "description": "The compiled plan: nodes, sources, execution units and edges.",
        "properties": {
            "metadata": {"$ref": _DEFS + "PlanMetadata"},
            "nodes": {"type": "object", "additionalProperties": {"$ref": _DEFS + "PlanNode"}},
            "sources": {"type": "object", "additionalProperties": {"$ref": _DEFS + "PlanSource"}},
            "units": {**ids, "description": "Unit id to its node ids, in run order."},
            "unit_parent_map": {**ids, "description": "Unit id to the unit ids it reads from."},
            "parent_map": {**ids, "description": "Unique id to its parents' unique ids."},
            "child_map": {**ids, "description": "Unique id to its children's unique ids."},
            "selectors": {
                "$ref": _DEFS + "PlanSelection",
                "description": "Empty lists in `compile --stable` output.",
            },
        },
        "required": [
            "metadata",
            "nodes",
            "sources",
            "units",
            "unit_parent_map",
            "parent_map",
            "child_map",
            "selectors",
        ],
        "$defs": dict(sorted(defs.items())),
    }
    return _document("manifest.json", MANIFEST_SCHEMA, "pipeloom manifest", body)


# File name under schema/v1/ to builder.
_BUILDERS: Final[Mapping[str, Callable[[], dict[str, Any]]]] = {
    "compile_result.json": compile_result_schema,
    "event.json": event_schema,
    "manifest.json": manifest_schema,
    "run_results.json": run_results_schema,
    "unit_graph.json": unit_graph_schema,
    "unit_states.json": unit_states_schema,
}


def artifact_schemas() -> dict[str, dict[str, Any]]:
    """Schemas by file name, as published under `schema/v1/`."""
    return {name: build() for name, build in _BUILDERS.items()}
