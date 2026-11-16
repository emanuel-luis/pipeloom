"""The published artifact schemas (ADR-0049): well formed, stable, and true to what pipeloom writes."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from jsonschema import Draft202012Validator, ValidationError

from pipeloom.artifacts.results import ROW_DEFAULTS
from pipeloom.artifacts.schema import (
    artifact_schemas,
    close,
    event_payloads,
    run_results_schema,
    unit_states_schema,
)
from pipeloom.cli import main
from pipeloom.config.schema import DIALECT, dump_schema
from pipeloom.runtime.finalize_run import parse_unit_states
from tests.conftest import GOLDENS, make_project
from tests.runtime.test_runner import shop
from tests.test_cli_run import ENV, MODELS, YML

SCHEMAS = artifact_schemas()
STRICT = {name: Draft202012Validator(close(doc)) for name, doc in SCHEMAS.items()}
BY_TAG = {doc["properties"]["schema"]["const"]: name for name, doc in SCHEMAS.items()}
# Fields the diagnostic goldens leave out because they change on every run.
FILLED = {"id": "e1", "ts": "2026-01-01T00:00:00.000000Z", "invocation_id": "inv"}


def row_validator() -> Draft202012Validator:
    """An attempt file is a run_results row: `run_results.json#/$defs/result`."""
    closed = close(run_results_schema())
    return Draft202012Validator(
        {"$schema": DIALECT, "$defs": closed["$defs"], "$ref": "#/$defs/result"}
    )


def documents() -> Iterator[tuple[str, dict[str, Any]]]:
    """Every golden document that carries one of the tags, and every event in a golden list.
    Read at collection time, so goldens added later are covered too."""
    for path in sorted(GOLDENS.rglob("*.json")):
        doc = json.loads(path.read_bytes().decode("utf-8"))
        rel = path.relative_to(GOLDENS).as_posix()
        items = doc if isinstance(doc, list) else [doc]
        for i, item in enumerate(items):
            if isinstance(item, dict) and item.get("schema") in BY_TAG:
                if BY_TAG[item["schema"]] == "event.json":
                    item = {**FILLED, **item}
                yield f"{rel}[{i}]", item


GOLDEN_DOCS = dict(documents())


@pytest.mark.parametrize("name", sorted(SCHEMAS))
def test_schemas_are_valid_draft_2020_12(name: str) -> None:
    doc = SCHEMAS[name]
    Draft202012Validator.check_schema(doc)
    Draft202012Validator.check_schema(close(doc))
    assert doc["$schema"] == DIALECT and doc["$id"].endswith(f"/schema/v1/{name}")
    assert "schema" in doc["required"] and "const" in doc["properties"]["schema"]
    assert '"additionalProperties": false' not in dump_schema(doc)  # published schemas are open


@pytest.mark.parametrize("name", sorted(SCHEMAS))
def test_schema_goldens(golden: Callable[[str, str], None], name: str) -> None:
    golden(f"artifact_schemas/{name}", dump_schema(SCHEMAS[name]))


def test_row_properties_match_the_row_defaults() -> None:
    row = run_results_schema()["$defs"]["result"]
    assert list(row["properties"]) == list(ROW_DEFAULTS)
    assert row["required"] == list(ROW_DEFAULTS)


def test_every_core_event_type_has_a_data_schema() -> None:
    conditions = SCHEMAS["event.json"]["allOf"]
    types = [c["if"]["properties"]["type"]["const"] for c in conditions]
    assert types == sorted(p.TYPE for p in event_payloads()) and len(types) == len(set(types))
    assert {"diagnostic", "unit.failed", "run.completed", "model.log"} <= set(types)


def test_close_closes_objects_but_not_conditionals() -> None:
    doc = {
        "type": "object",
        "properties": {"a": {"type": "object", "properties": {"b": {}}}},
        "allOf": [{"if": {"properties": {"a": {}}}, "then": {"properties": {"a": {}}}}],
    }
    closed = close(doc)
    assert closed["additionalProperties"] is False
    assert closed["properties"]["a"]["additionalProperties"] is False
    assert "additionalProperties" not in closed["allOf"][0]["if"]
    assert "additionalProperties" not in closed["allOf"][0]["then"]
    assert "additionalProperties" not in doc  # the input is not changed


def test_the_goldens_include_run_results_and_events() -> None:
    tags = {doc["schema"] for doc in GOLDEN_DOCS.values()}
    assert {"pipeloom.run_results/1", "pipeloom.event/1"} <= tags


@pytest.mark.parametrize("key", sorted(GOLDEN_DOCS))
def test_golden_documents_validate_strictly(key: str) -> None:
    doc = GOLDEN_DOCS[key]
    STRICT[BY_TAG[doc["schema"]]].validate(doc)


@pytest.mark.parametrize("path", sorted((GOLDENS / "events").glob("*.json")), ids=lambda p: p.name)
def test_event_payload_goldens_validate_strictly(path: Path) -> None:
    """`events/<type with _>.json` holds the data of one event of that type."""
    by_stem = {p.TYPE.replace(".", "_"): p.TYPE for p in event_payloads()}
    data = json.loads(path.read_bytes().decode("utf-8"))
    event = {
        **FILLED,
        "schema": "pipeloom.event/1",
        "run_id": "r1",
        "seq": 1,
        "type": by_stem[path.stem],
        "level": "info",
        "data": data,
    }
    STRICT["event.json"].validate(event)
    with pytest.raises(ValidationError, match="'unexpected' was unexpected"):
        STRICT["event.json"].validate({**event, "data": {**data, "unexpected": 1}})


def lines(path: Path) -> list[Any]:
    return [json.loads(ln) for ln in path.read_bytes().decode("utf-8").splitlines() if ln]


@pytest.mark.parametrize("project", ["shop", "failing"])
def test_a_real_run_writes_only_valid_artifacts(tmp_path: Path, project: str) -> None:
    root = tmp_path / project
    if project == "shop":
        shop(root)  # tests/fixtures/shop with its source tables
    else:
        make_project(root, {"pipeloom.yml": YML, "models/m.yml": MODELS})
    argv = ["run", "--project-dir", str(root), "--target", "ci", "--log-format", "json"]
    done = CliRunner().invoke(main, [*argv, "--run-id", "r1"], env=ENV)
    assert done.exit_code == (0 if project == "shop" else 1), done.stderr
    run_dir = root / "target" / "runs" / "r1"
    events = [json.loads(ln) for ln in done.stderr.splitlines() if ln.startswith("{")]
    for file in run_dir.glob("events/*.jsonl"):
        events += lines(file)
    assert {"run.started", "unit.succeeded", "run.completed"} <= {e["type"] for e in events}
    for event in events:
        STRICT["event.json"].validate(event)
    for results in (run_dir / "run_results.json", root / "target" / "run_results.json"):
        STRICT["run_results.json"].validate(json.loads(results.read_bytes().decode("utf-8")))
    attempts = sorted(run_dir.glob("units/*/attempt-*.json"))
    assert attempts
    rows = row_validator()
    for attempt in attempts:
        rows.validate(json.loads(attempt.read_bytes().decode("utf-8")))
    if project == "failing":
        assert "unit.failed" in {e["type"] for e in events}


def test_an_unknown_event_type_keeps_an_open_data_object() -> None:
    event = {
        **FILLED,
        "schema": "pipeloom.event/1",
        "run_id": None,
        "seq": 1,
        "type": "plugin.custom",
        "level": "info",
        "data": {"anything": [1, "two"]},
    }
    STRICT["event.json"].validate(event)
    with pytest.raises(ValidationError, match="is not one of"):
        STRICT["event.json"].validate({**event, "level": "fatal"})


def test_an_unknown_skip_reason_is_valid() -> None:
    """Skip reasons are open strings in events, as in the run_results rows."""
    event = {
        **FILLED,
        "schema": "pipeloom.event/1",
        "run_id": "r1",
        "seq": 1,
        "type": "unit.skipped",
        "level": "info",
        "data": {"reason": "paused_by_operator", "upstream": None},
    }
    Draft202012Validator(SCHEMAS["event.json"]).validate(event)
    STRICT["event.json"].validate(event)
    with pytest.raises(ValidationError, match="is not of type 'string'"):
        STRICT["event.json"].validate({**event, "data": {"reason": 1, "upstream": None}})


UNIT_STATES = [
    {
        "schema": "pipeloom.unit_states/1",
        "units": {
            "top_customers": {
                "status": "failed",
                "code": "PLX106",
                "message": "runner lost",
                "orchestrator_ids": {"gha_run_id": "123"},
            },
            "b": {"status": "skipped", "reason": None},
            "c": {"status": "excluded"},
        },
    },
    {"schema": "pipeloom.unit_states/1", "units": {}},
    {"schema": "pipeloom.unit_states/1", "units": {"a": {"status": "failed", "later": 1}}},
    {"schema": "pipeloom.unit_states/2", "units": {}},
    {"schema": "pipeloom.unit_states/1", "units": []},
    {"schema": "pipeloom.unit_states/1"},
    {"schema": "pipeloom.unit_states/1", "units": {"a": {"status": "done"}}},
    {"schema": "pipeloom.unit_states/1", "units": {"a": {}}},
    {"schema": "pipeloom.unit_states/1", "units": {"a": {"status": "failed", "code": 7}}},
    {
        "schema": "pipeloom.unit_states/1",
        "units": {"a": {"status": "failed", "orchestrator_ids": {"Bad": "x"}}},
    },
    {
        "schema": "pipeloom.unit_states/1",
        "units": {"a": {"status": "failed", "orchestrator_ids": {"k": ""}}},
    },
]


@pytest.mark.parametrize("doc", UNIT_STATES, ids=lambda d: json.dumps(d)[:60])
def test_unit_states_schema_agrees_with_the_reader(doc: dict[str, Any]) -> None:
    """The published (open) schema accepts exactly what `pipeloom finalize` reads."""
    open_ok = Draft202012Validator(unit_states_schema()).is_valid(doc)
    try:
        parse_unit_states(json.dumps(doc).encode("utf-8"))
        read_ok = True
    except ValueError:
        read_ok = False
    assert open_ok == read_ok
    if "later" in json.dumps(doc):  # an unknown field: fine for consumers, a bug for pipeloom
        assert open_ok and not STRICT["unit_states.json"].is_valid(doc)
