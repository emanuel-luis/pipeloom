"""Export JSON Schemas for pipeloom.yml, model files and `_defaults.yml`.

The schemas come from the pydantic models. Plugin blocks are open in the models (a plugin's
options are only known once it is installed), so the export replaces them with a `oneOf` that has
one branch per known plugin kind.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, Final

from pydantic import BaseModel
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaValue
from pydantic_core import core_schema

from pipeloom.config.models import DefaultsFile, ModelFile, ProjectConfig
from pipeloom.plugins import Group, PluginSpec

SCHEMA_BASE_URL: Final = "https://emanuel-luis.github.io/pipeloom/latest/schema/v1/"
DIALECT: Final = "https://json-schema.org/draft/2020-12/schema"
INTERPOLATION_PATTERN: Final = r"^\$\{.+\}$"

# Runtime keys that hold a plugin block, and the group each selects from.
_BLOCK_FIELDS: Final[Mapping[str, Group]] = {
    "engine": "engines",
    "catalog": "catalogs",
    "naming": "naming",
    "state": "state_stores",
    "deploy": "renderers",
}


def _or_interpolation(schema: JsonSchemaValue) -> JsonSchemaValue:
    return {"anyOf": [schema, {"type": "string", "pattern": INTERPOLATION_PATTERN}]}


class _Generator(GenerateJsonSchema):
    """Numbers and booleans may also be written as an interpolation such as ${env:WORKERS}."""

    schema_dialect = DIALECT

    def int_schema(self, schema: core_schema.IntSchema) -> JsonSchemaValue:
        return _or_interpolation(super().int_schema(schema))

    def float_schema(self, schema: core_schema.FloatSchema) -> JsonSchemaValue:
        return _or_interpolation(super().float_schema(schema))

    def bool_schema(self, schema: core_schema.BoolSchema) -> JsonSchemaValue:
        return _or_interpolation(super().bool_schema(schema))


def _model_schema(model: type[BaseModel]) -> dict[str, Any]:
    return model.model_json_schema(schema_generator=_Generator, mode="validation")


def _rename_refs(node: Any, prefix: str) -> Any:
    if isinstance(node, dict):
        out = {k: _rename_refs(v, prefix) for k, v in node.items()}
        ref = out.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            out["$ref"] = "#/$defs/" + prefix + ref.removeprefix("#/$defs/")
        return out
    if isinstance(node, list):
        return [_rename_refs(v, prefix) for v in node]
    return node


def _branches(
    specs: Sequence[PluginSpec[Any]],
    defs: dict[str, Any],
    base_props: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """One object schema per plugin kind; its nested definitions are hoisted into `defs`."""
    out: list[dict[str, Any]] = []
    for spec in sorted(specs, key=lambda s: s.kind):
        schema = _model_schema(spec.config_model)
        prefix = f"{spec.group}.{spec.kind}."
        for name, sub in schema.pop("$defs", {}).items():
            defs[prefix + name] = _rename_refs(sub, prefix)
        schema = _rename_refs(schema, prefix)
        kind_prop: dict[str, Any] = {"const": spec.kind}
        if spec.description:
            kind_prop["description"] = spec.description
        branch: dict[str, Any] = {
            "type": "object",
            "title": spec.kind,
            "properties": {
                "kind": kind_prop,
                **(base_props or {}),
                **schema.get("properties", {}),
            },
            "required": ["kind", *schema.get("required", [])],
            "additionalProperties": schema.get("additionalProperties", True),
        }
        out.append(branch)
    return out


def _inject_plugins(
    schema: dict[str, Any], specs: Mapping[Group, Sequence[PluginSpec[Any]]]
) -> None:
    defs: dict[str, Any] = schema["$defs"]
    holders = [schema["properties"], defs["TargetConfig"]["properties"]]
    for field, group in _BLOCK_FIELDS.items():
        if not specs.get(group):
            continue
        branches = _branches(specs[group], defs)
        for props in holders:
            old = props[field]
            new: dict[str, Any] = {"oneOf": [*branches, {"type": "null"}]}
            if "description" in old:
                new["description"] = old["description"]
            props[field] = new
    if specs.get("sinks"):
        core = {k: v for k, v in defs["SinkBlock"]["properties"].items() if k != "kind"}
        sinks = defs["EventsConfig"]["properties"]["sinks"]
        sinks["items"] = {"oneOf": _branches(specs["sinks"], defs, core)}


def _finish(schema: dict[str, Any], file: str, title: str) -> dict[str, Any]:
    schema["$schema"] = DIALECT
    schema["$id"] = SCHEMA_BASE_URL + file
    schema["title"] = title
    return schema


def export_schemas(
    specs: Mapping[Group, Sequence[PluginSpec[Any]]],
) -> dict[str, dict[str, Any]]:
    """Schemas by file name: project.json, models.json and defaults.json."""
    project = _model_schema(ProjectConfig)
    _inject_plugins(project, specs)
    return {
        "project.json": _finish(project, "project.json", "pipeloom project"),
        "models.json": _finish(_model_schema(ModelFile), "models.json", "pipeloom model file"),
        "defaults.json": _finish(
            _model_schema(DefaultsFile), "defaults.json", "pipeloom _defaults.yml"
        ),
    }


def dump_schema(schema: Mapping[str, Any]) -> str:
    """Deterministic text: sorted keys, two-space indent, ASCII, one trailing LF."""
    return json.dumps(schema, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
