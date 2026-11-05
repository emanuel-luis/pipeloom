from __future__ import annotations

import json
from pathlib import Path

from pipeloom.events.model import Event
from pipeloom.runtime.finalize_run import ExternalUnitState, read_unit_states
from pipeloom_airflow.states import (
    ChildTerminal,
    parse_child_line,
    unit_states,
    unit_states_document,
)

TASKS = {"t1": ("u1",), "t2": ("u2", "u3"), "t3": ("u4",), "t4": ("u5",), "t5": ("u6",)}


def test_the_state_table() -> None:
    out = unit_states(
        {
            "t1": "success",
            "t2": "failed",
            "t3": "upstream_failed",
            "t4": "skipped",
            "t5": "running",
        },
        TASKS,
    )
    assert sorted(out) == ["u1", "u2", "u3", "u4", "u5"]
    assert out["u1"].status == "failed" and out["u1"].code == "PLA004"
    assert "no attempt file is visible under artifacts.root" in (out["u1"].message or "")
    assert out["u2"] == out["u3"] and out["u2"].message == "task 't2' ended without a unit result"
    assert (out["u4"].status, out["u4"].reason, out["u4"].code) == (
        "skipped",
        "upstream_failed",
        None,
    )
    assert (out["u5"].status, out["u5"].reason) == ("skipped", "not_run")


def test_unknown_and_missing_states_are_left_out() -> None:
    assert unit_states({"t1": None, "t2": "up_for_retry"}, TASKS) == {}


def test_the_document_round_trips_through_the_core_reader(tmp_path: Path) -> None:
    states = {
        "a": ExternalUnitState("failed", code="PLA004", message="m", orchestrator_ids={"k": "v"}),
        "b": ExternalUnitState("skipped", reason="not_run"),
    }
    full = unit_states_document(states, compact=False)
    assert full["units"] == {
        "a": {
            "status": "failed",
            "code": "PLA004",
            "message": "m",
            "orchestrator_ids": {"k": "v"},
        },
        "b": {"status": "skipped", "reason": "not_run"},
    }
    compact = unit_states_document(states, compact=True)
    assert compact["units"] == {
        "a": {"status": "failed", "code": "PLA004", "message": "m"},
        "b": {"status": "skipped", "reason": "not_run"},
    }
    path = tmp_path / "s.json"
    path.write_text(json.dumps(full), encoding="utf-8")
    assert read_unit_states(path) == states
    assert full["schema"] == "pipeloom.unit_states/1"


EVENTS = Path(__file__).parent / "data" / "child.jsonl"


def test_parse_child_line_over_recorded_events() -> None:
    lines = EVENTS.read_text(encoding="utf-8").splitlines()
    for line in lines:
        if line.startswith("{"):
            Event.model_validate_json(line)  # the recording is a valid event
    got = [parse_child_line(ln) for ln in lines]
    assert [t for t, _ in got] == [
        None,
        None,
        ChildTerminal("failed", "PLX201", "permanent", "boom"),
        ChildTerminal("succeeded", None, None, None),
        ChildTerminal("skipped", None, None, "upstream_failed"),
        None,
    ]
    assert got[2][1] == "unit.failed stg_orders boom"
    assert got[1][1] == "unit.started stg_orders"
    assert got[5][1] == "plain text from the child"


def test_a_json_line_that_is_not_an_event_passes_through() -> None:
    assert parse_child_line('{"a": 1}\n') == (None, '{"a": 1}')
    assert parse_child_line("[1]") == (None, "[1]")
