from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from pipeloom.config.schema import SCHEMA_BASE_URL, dump_schema, export_schemas
from pipeloom.diagnostics import Report
from pipeloom.plugins import PluginRegistry
from tests.conftest import FIXTURES
from tests.plugins.fakes import FAKE_ENGINE, FAKE_SINK

MINIMAL = FIXTURES / "minimal"
FILES = ("project.json", "properties.json")


def builtin_schemas() -> dict[str, dict[str, Any]]:
    report = Report()
    specs = PluginRegistry.discover(autoload=False).load_all(report)
    assert report.sorted() == ()
    return export_schemas(specs)


def validator(schema: dict[str, Any]) -> Draft202012Validator:
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def load_yaml(path: Path) -> Any:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.mark.parametrize("name", FILES)
def test_builtin_schema_goldens(golden: Callable[[str, str], None], name: str) -> None:
    golden(f"schema/{name}", dump_schema(builtin_schemas()[name]))


def test_schema_with_third_party_plugins_golden(golden: Callable[[str, str], None]) -> None:
    schemas = export_schemas({"engines": [FAKE_ENGINE], "sinks": [FAKE_SINK]})
    golden("schema/project.plugins.json", dump_schema(schemas["project.json"]))


def test_export_is_deterministic_lf_and_ascii() -> None:
    first = {k: dump_schema(v) for k, v in builtin_schemas().items()}
    second = {k: dump_schema(v) for k, v in builtin_schemas().items()}
    assert first == second
    for text in first.values():
        assert "\r" not in text and text.endswith("}\n") and text.isascii()
        assert json.loads(text)["$schema"] == "https://json-schema.org/draft/2020-12/schema"


def test_ids_use_the_public_base_url() -> None:
    assert SCHEMA_BASE_URL == "https://emanuel-luis.github.io/pipeloom/latest/schema/v1/"
    for name, schema in builtin_schemas().items():
        assert schema["$id"] == SCHEMA_BASE_URL + name


def test_the_fixture_project_validates_against_the_exported_schemas() -> None:
    schemas = builtin_schemas()
    validator(schemas["project.json"]).validate(load_yaml(MINIMAL / "pipeloom.yml"))


def project(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "config_version": 1,
        "name": "p",
        "targets": {"dev": {"engine": {"kind": "memory"}}},
    }
    return {**base, **overrides}


def errors(schema: dict[str, Any], doc: Any) -> list[str]:
    return [e.message for e in validator(schema).iter_errors(doc)]


def test_plugin_blocks_are_one_of_with_kind_const() -> None:
    schema = builtin_schemas()["project.json"]
    engine = schema["$defs"]["TargetConfig"]["properties"]["engine"]
    branches = [b for b in engine["oneOf"] if "properties" in b]
    assert [b["properties"]["kind"]["const"] for b in branches] == ["memory"]
    naming = schema["properties"]["naming"]["oneOf"]
    assert [b["properties"]["kind"]["const"] for b in naming if "properties" in b] == [
        "none",
        "per_user",
        "prefix",
    ]
    assert errors(schema, project(naming={"kind": "per_user", "schema": "dev"})) == []


def test_unknown_kind_and_bad_options_are_rejected() -> None:
    schema = builtin_schemas()["project.json"]
    assert errors(schema, project(targets={"dev": {"engine": {"kind": "nope"}}}))
    assert errors(schema, project(naming={"kind": "prefix", "strategy": "x"}))
    assert errors(schema, project(bogus=1))
    assert errors(schema, project(targets={}))


def test_third_party_options_appear_in_the_schema() -> None:
    schema = export_schemas({"engines": [FAKE_ENGINE], "sinks": [FAKE_SINK]})["project.json"]
    good = project(targets={"dev": {"engine": {"kind": "fake", "path": "a", "size": 3}}})
    assert errors(schema, good) == []
    bad = project(targets={"dev": {"engine": {"kind": "fake", "size": "x"}}})
    assert errors(schema, bad)
    sink_ok = good | {"events": {"sinks": [{"kind": "tap", "buffered": True, "path": "p"}]}}
    assert errors(schema, sink_ok) == []
    assert errors(schema, good | {"events": {"sinks": [{"kind": "other"}]}})


def test_without_plugins_a_block_stays_open() -> None:
    schema = export_schemas({})["project.json"]
    doc = project(targets={"dev": {"engine": {"kind": "anything", "x": [1]}}})
    assert errors(schema, doc) == []


@pytest.mark.parametrize(
    "doc",
    [
        project(workers="${env:WORKERS}"),
        project(workers=2, ephemeral_fanout_warn="${var:n}"),
        project(models={"+timeout": "1h30m", "+retries": "${env:R}"}),
        project(models={"+timeout": 90, "+retry_delay": "${env:D}"}),
        project(models={"+quality": {"max_drop_ratio": "${var:r}"}}),
    ],
)
def test_numbers_and_booleans_accept_interpolation(doc: dict[str, Any]) -> None:
    assert errors(builtin_schemas()["project.json"], doc) == []


@pytest.mark.parametrize(
    "doc",
    [
        project(workers="many"),
        project(workers=0),
        project(models={"+timeout": "soon"}),
        project(models={"+timeout": ""}),
    ],
)
def test_other_strings_are_still_rejected(doc: dict[str, Any]) -> None:
    assert errors(builtin_schemas()["project.json"], doc)


def test_project_schema_has_the_models_tree_and_source_groups() -> None:
    schema = builtin_schemas()["project.json"]
    tree = {"+materialized": "view", "silver": {"+tags": ["s"], "deep": {"+retries": 1}}}
    groups = {"tpch": {"format": "parquet", "tables": {"orders": {"location": "o"}, "c": None}}}
    override = {"sources": {"tpch": {"location": None, "tables": {"orders": {"location": None}}}}}
    good = project(models=tree, sources=groups)
    good["targets"]["prod"] = {"engine": {"kind": "memory"}, **override}
    assert errors(schema, good) == []
    assert errors(schema, project(models={"+materalized": "view"}))  # not a folder mapping
    assert errors(schema, project(models={"silver": {"+retries": "x"}}))
    assert errors(schema, project(sources={"g": {"tables": {"t": {"bogus": 1}}}}))


def test_properties_schema() -> None:
    schema = builtin_schemas()["properties.json"]
    entry = {
        "name": "m",
        "description": "d",
        "config": {"retries": 2},
        "columns": [{"name": "id", "dtype": "int64"}],
        "data_tests": [{"name": "t", "check": {"unique": ["id"]}}],
        "unit_tests": [{"name": "u", "given": {"a": {"rows": []}}, "expect": {"rows": []}}],
    }
    assert errors(schema, {"models": [entry]}) == []
    assert errors(schema, {"models": [{"name": "m", "data_tests": [{"name": "t"}]}]})
    assert errors(schema, {"models": [{"name": "m", "inputs": {}}]})
    assert errors(schema, {"models": [{"name": "m", "sql": "select 1"}]})


def test_only_the_project_and_properties_schemas_are_exported() -> None:
    assert sorted(builtin_schemas()) == sorted(FILES)
