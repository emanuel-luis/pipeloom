from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from pipeloom.events.model import (
    EVENT_SCHEMA,
    ArtifactWritten,
    DiagnosticEmitted,
    Event,
    EventPayload,
    InvocationCompleted,
    InvocationStarted,
    Level,
    PlanCompiled,
    SinkDisabled,
)
from pipeloom.testing import FakeClock, RecordingSink


def make(ts: datetime) -> Event:
    return Event(
        id="a",
        run_id=None,
        invocation_id="i",
        seq=1,
        ts=ts,
        type="t",
        level=Level.INFO,
        data={"k": [1, {"x": None}]},
    )


def test_envelope_json_has_the_schema_key_and_utc_microseconds() -> None:
    zone = timezone(timedelta(hours=-3))
    raw = make(datetime(2026, 1, 2, 3, 4, 5, tzinfo=zone)).to_json()
    assert raw["schema"] == EVENT_SCHEMA == "pipeloom.event/1"
    assert raw["ts"] == "2026-01-02T06:04:05.000000Z"
    assert set(raw) == {
        "schema", "id", "run_id", "invocation_id", "seq", "ts", "type", "level",
        "unit_id", "model", "attempt", "data", "context",
    }  # fmt: skip


def test_schema_value_is_checked_and_event_is_frozen() -> None:
    raw = make(datetime(2026, 1, 1, tzinfo=UTC)).to_json()
    assert Event.model_validate(raw).seq == 1
    with pytest.raises(ValidationError):
        Event.model_validate({**raw, "schema": "pipeloom.event/2"})
    with pytest.raises(ValidationError):
        make(datetime(2026, 1, 1, tzinfo=UTC)).seq = 2  # type: ignore[misc]


def test_payload_types_and_levels() -> None:
    payloads: list[type[EventPayload]] = [
        InvocationStarted,
        DiagnosticEmitted,
        PlanCompiled,
        ArtifactWritten,
        SinkDisabled,
        InvocationCompleted,
    ]
    assert [p.TYPE for p in payloads] == [
        "invocation.started",
        "diagnostic",
        "plan.compiled",
        "artifact.written",
        "sink.disabled",
        "invocation.completed",
    ]
    assert (
        SinkDisabled.LEVEL is Level.WARNING
        and SinkDisabled.model_fields["code"].default == "PLE401"
    )


def test_payload_data_is_a_json_object() -> None:
    data = PlanCompiled(
        project="p",
        target="t",
        plan_hash="h",
        units=1,
        by_materialization={"table": 1},
        ephemeral_inlined=0,
        selected=1,
    ).to_data()
    assert json.loads(json.dumps(data)) == data and isinstance(data, dict)


def test_fake_clock_and_recording_sink_helpers() -> None:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), timedelta(seconds=2))
    first, second = clock.now(), clock.now()
    assert second - first == timedelta(seconds=2)
    clock.advance(10)
    assert clock.monotonic() == 14.0
    with pytest.raises(ValueError, match="aware"):
        FakeClock(datetime(2026, 1, 1))
    rec = RecordingSink()
    for t in ("a", "b", "c"):
        rec.emit(make(first).model_copy(update={"type": t}))
    rec.assert_sequence("a", "c")
    with pytest.raises(AssertionError):
        rec.assert_sequence("c", "a")
