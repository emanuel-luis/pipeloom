"""The event envelope (`pipeloom.event/1`) and the payloads pipeloom 0.1 emits."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, ClassVar, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_serializer

from pipeloom.diagnostics import Diagnostic, Severity

EVENT_SCHEMA: Final = "pipeloom.event/1"


class Level(StrEnum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class Event(BaseModel):
    """One event. `data` is the payload as a JSON object. Serialize with `by_alias=True`."""

    model_config = ConfigDict(frozen=True, populate_by_name=True)

    schema_: Literal["pipeloom.event/1"] = Field(default=EVENT_SCHEMA, alias="schema")
    id: str
    run_id: str | None
    invocation_id: str
    seq: int
    ts: datetime
    type: str
    level: Level
    unit_id: str | None = None
    model: str | None = None
    attempt: int | None = None
    data: dict[str, JsonValue]
    context: dict[str, str] = Field(default_factory=dict)

    @field_serializer("ts")
    def _ts(self, value: datetime) -> str:
        """RFC 3339 in UTC, always with microseconds."""
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")

    def to_json(self) -> dict[str, Any]:
        """The envelope as a JSON-ready dict, with the `schema` key."""
        out: dict[str, Any] = self.model_dump(mode="json", by_alias=True)
        return out


class EventPayload(BaseModel):
    """Base for payloads. `TYPE` is the event type and `LEVEL` its default level."""

    model_config = ConfigDict(frozen=True)

    TYPE: ClassVar[str]
    LEVEL: ClassVar[Level] = Level.INFO

    def event_level(self) -> Level:
        """The level this payload emits at; payloads with a variable level override it."""
        return self.LEVEL

    def to_data(self) -> dict[str, Any]:
        out: dict[str, Any] = self.model_dump(mode="json")
        return out


class InvocationStarted(EventPayload):
    TYPE = "invocation.started"

    command: str
    args: dict[str, JsonValue]
    pipeloom_version: str
    project: str | None
    target: str | None


class DiagnosticEmitted(EventPayload):
    TYPE = "diagnostic"

    code: str
    severity: str
    message: str
    hint: str | None
    location: dict[str, JsonValue] | None
    context: dict[str, JsonValue]

    @classmethod
    def from_diagnostic(cls, d: Diagnostic) -> DiagnosticEmitted:
        return cls.model_validate(d.to_json())

    def to_diagnostic(self) -> Diagnostic:
        return Diagnostic.from_json(self.to_data())

    def event_level(self) -> Level:
        return {
            Severity.ERROR.value: Level.ERROR,
            Severity.WARNING.value: Level.WARNING,
        }.get(self.severity, Level.INFO)


class PlanCompiled(EventPayload):
    TYPE = "plan.compiled"

    project: str
    target: str
    plan_hash: str
    units: int
    by_materialization: dict[str, int]
    ephemeral_inlined: int
    selected: int


class ArtifactWritten(EventPayload):
    TYPE = "artifact.written"

    path: str
    kind: str
    bytes: int
    sha256: str


class SinkDisabled(EventPayload):
    TYPE = "sink.disabled"
    LEVEL = Level.WARNING

    sink: str
    failures: int
    error: str
    code: str = "PLE401"


class InvocationCompleted(EventPayload):
    TYPE = "invocation.completed"

    command: str
    exit_code: int
    duration_ms: int
    counts: dict[str, int]


class UnitQueued(EventPayload):
    TYPE = "unit.queued"

    downstream: int
    """Selected units that depend on this one, transitively."""


class UnitSkipped(EventPayload):
    TYPE = "unit.skipped"

    reason: Literal["upstream_failed", "fail_fast"]
    upstream: str | None
    """The failed unit that caused the skip; None for `fail_fast`."""


class UnitCancelled(EventPayload):
    TYPE = "unit.cancelled"
    LEVEL = Level.WARNING

    reason: Literal["cancel", "timeout"] = "cancel"
