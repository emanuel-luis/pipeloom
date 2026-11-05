"""From Airflow task states to the unit states `pipeloom finalize` takes, and a child's output."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass

from pydantic import JsonValue

from pipeloom.runtime.finalize_run import UNIT_STATES_SCHEMA, ExternalUnitState

NO_FILE = "succeeded but no attempt file is visible under artifacts.root (the root must be shared)"
NO_RESULT = "ended without a unit result"


def unit_states(
    task_states: Mapping[str, str | None], task_units: Mapping[str, tuple[str, ...]]
) -> dict[str, ExternalUnitState]:
    """What the DAG run knows about units that wrote no attempt file. The attempt files win in
    the core, so these apply only when a file is missing. Other states are left out."""
    out: dict[str, ExternalUnitState] = {}
    for task, units in task_units.items():
        state = task_states.get(task)
        made: ExternalUnitState | None = None
        if state == "success":
            made = ExternalUnitState("failed", code="PLA004", message=f"task '{task}' {NO_FILE}")
        elif state == "failed":
            made = ExternalUnitState("failed", code="PLA004", message=f"task '{task}' {NO_RESULT}")
        elif state == "upstream_failed":
            made = ExternalUnitState("skipped", reason="upstream_failed")
        elif state == "skipped":
            made = ExternalUnitState("skipped", reason="not_run")
        if made is not None:
            out.update(dict.fromkeys(units, made))
    return out


def unit_states_document(
    states: Mapping[str, ExternalUnitState], *, compact: bool
) -> dict[str, JsonValue]:
    """A `pipeloom.unit_states/1` document. `compact` leaves out `orchestrator_ids` and shortens
    the PLA004 messages (the long text is in the ADR and the README), so it stays small; status,
    reason and code are always kept."""
    units: dict[str, JsonValue] = {}
    for unit, s in sorted(states.items()):
        row: dict[str, JsonValue] = {"status": s.status}
        message = s.message
        if compact and s.code == "PLA004" and message is not None:
            if message.endswith(NO_FILE):
                message = "no attempt file visible under artifacts.root"
            elif message.endswith(NO_RESULT):
                message = NO_RESULT
        for name, value in (("reason", s.reason), ("code", s.code), ("message", message)):
            if value is not None:
                row[name] = value
        if not compact and s.orchestrator_ids:
            row["orchestrator_ids"] = dict(sorted(s.orchestrator_ids.items()))
        units[unit] = row
    return {"schema": UNIT_STATES_SCHEMA, "units": units}


@dataclass(frozen=True)
class ChildTerminal:
    status: str
    code: str | None
    category: str | None
    message: str | None


_TERMINAL = {
    "unit.succeeded": "succeeded",
    "unit.failed": "failed",
    "unit.timed_out": "timed_out",
    "unit.cancelled": "cancelled",
    "unit.skipped": "skipped",
}


def parse_child_line(line: str) -> tuple[ChildTerminal | None, str]:
    """One line of a child's `--log-format json` stderr: the terminal unit event it carries
    (None for any other line) and the text to log. A line that is not an event passes through."""
    text = line.rstrip("\r\n")
    try:
        event = json.loads(text)
    except ValueError:
        return None, text
    if not isinstance(event, dict) or event.get("schema") != "pipeloom.event/1":
        return None, text
    kind = str(event.get("type", ""))
    data = event.get("data")
    data = data if isinstance(data, dict) else {}
    error = data.get("error")
    error = error if isinstance(error, dict) else {}
    message = error.get("message")
    shown = " ".join(p for p in (kind, str(event.get("unit_id") or ""), str(message or "")) if p)
    status = _TERMINAL.get(kind)
    if status is None:
        return None, shown
    reason = data.get("reason")
    return (
        ChildTerminal(
            status,
            error.get("code") if isinstance(error.get("code"), str) else None,
            error.get("category") if isinstance(error.get("category"), str) else None,
            message if isinstance(message, str) else (reason if isinstance(reason, str) else None),
        ),
        shown,
    )
