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

    reason: Literal["upstream_failed", "fail_fast", "not_run"]
    upstream: str | None
    """The failed unit that caused the skip; None for `fail_fast`."""


class UnitCancelled(EventPayload):
    TYPE = "unit.cancelled"
    LEVEL = Level.WARNING

    reason: Literal["cancel", "timeout"] = "cancel"


class UnitTimedOut(EventPayload):
    TYPE = "unit.timed_out"
    LEVEL = Level.ERROR

    timeout_s: float
    attempt: int


class ErrorInfo(BaseModel):
    """Why a unit failed. `category` tells a retry policy whether trying again can help."""

    model_config = ConfigDict(frozen=True)

    code: str
    category: Literal["transient", "permanent"]
    message: str
    hint: str | None = None
    exception_type: str | None = None
    trace_excerpt: list[str] = Field(default_factory=list)
    """Frames under the project dir, then the root cause."""
    location: dict[str, JsonValue] | None = None


class UnitRetrying(EventPayload):
    TYPE = "unit.retrying"
    LEVEL = Level.WARNING

    attempt: int
    """The attempt that failed."""
    next_attempt: int
    delay_ms: int
    error: ErrorInfo | None = None


class RunStarted(EventPayload):
    TYPE = "run.started"

    project: str
    target: str
    plan_hash: str
    selected: int
    """Units this invocation runs."""
    workers: int
    boundary: Literal["process", "task"]
    executor: str
    full_refresh: bool
    fail_fast: bool
    engine: str
    engine_version: str
    parent_run_id: str | None = None
    """The run this one retries."""


class RunCompiled(EventPayload):
    TYPE = "run.compiled"

    units: int
    by_materialization: dict[str, int]
    ephemeral_inlined: int
    selected: int
    plan_hash: str
    executor: str


class RunCancelRequested(EventPayload):
    TYPE = "run.cancel_requested"
    LEVEL = Level.WARNING

    signal: str | None
    """SIGINT, SIGTERM or SIGBREAK when a signal asked for the stop; None for an API call."""
    grace_s: float
    """Seconds running units get to finish before they are marked cancelled."""


class RunCompleted(EventPayload):
    TYPE = "run.completed"

    status: Literal["succeeded", "failed", "cancelled"]
    exit_code: int
    duration_ms: int
    counts: dict[str, int]
    rows_written: int
    bytes_written: int
    failures: list[dict[str, JsonValue]]
    """One entry per failed unit: unit id and the diagnostic."""
    artifacts: list[str]
    """POSIX paths of the files the run wrote."""
    abandoned_threads: int = 0
    """Timed-out unit threads that were still running when the run ended."""
    replay: bool = False
    """`pipeloom finalize` found a finished record of this run with the same status in the
    state store and no unit attempt started after it: the finalize was retried and reports
    nothing new. `pipeloom run` always sets
    false, since it runs units again."""

    def event_level(self) -> Level:
        return Level.INFO if self.status == "succeeded" else Level.ERROR


class UnitStarted(EventPayload):
    TYPE = "unit.started"

    materialization: str
    executor: str
    queue_wait_ms: int


class UnitSucceeded(EventPayload):
    TYPE = "unit.succeeded"

    materialization: str
    strategy: str | None
    relation: str | None
    rows_written: int | None
    rows_inserted: int | None
    bytes_written: int | None
    duration_ms: int


class UnitFailed(EventPayload):
    TYPE = "unit.failed"
    LEVEL = Level.ERROR

    error: ErrorInfo
    duration_ms: int
    traceback: str | None = None
    """The full traceback; the only place it appears."""


class ModelInputRead(EventPayload):
    TYPE = "model.input_read"

    alias: str
    kind: Literal["source", "model", "ephemeral"]
    name: str
    rows: int | None
    """None for a lazy relation, which is not counted."""
    duration_ms: int
    relation: str | None = None
    """The relation the read used, set when the read was deferred."""
    deferred: bool = False
    """True when `--defer` read a parent from the reference's relation."""


class ModelWritten(EventPayload):
    TYPE = "model.written"

    relation: str
    materialization: str
    strategy: str | None
    rows_written: int | None
    rows_inserted: int | None
    bytes_written: int | None
    duration_ms: int
    skipped: bool = False
    """The write token had been applied before: nothing was written."""


class StateWatermarkCommitted(EventPayload):
    TYPE = "state.watermark_committed"

    column: str
    dtype: str
    before: JsonValue
    after: JsonValue
    source: Literal["state", "target"]
    seeded: bool


class ModelSchemaChanged(EventPayload):
    TYPE = "model.schema_changed"

    relation: str
    policy: str
    action: Literal["applied", "ignored", "rejected", "replaced"]
    changes: list[dict[str, JsonValue]]
    """The diff from the target to the batch: kind, column, old, new."""

    def event_level(self) -> Level:
        return Level.WARNING if self.action == "rejected" else Level.INFO


class ModelQualityEvaluated(EventPayload):
    TYPE = "model.quality_evaluated"

    total_rows: int
    results: list[dict[str, JsonValue]]
    """One entry per expectation: name, kind, action, passed, failed, value."""
    rows_dropped: int
    rows_quarantined: int
    drop_ratio: float
    cached: bool
    """The output was persisted before it was counted."""
    failures: list[str]
    """Expectations with action fail that failed, and `max_drop_ratio` when exceeded."""

    def event_level(self) -> Level:
        noisy = self.failures or any(
            r["action"] in ("warn", "drop") and r["failed"] for r in self.results
        )
        return Level.WARNING if noisy else Level.INFO


class ModelLog(EventPayload):
    TYPE = "model.log"

    message: str
    level: Literal["debug", "info", "warning"] = "info"
    fields: dict[str, JsonValue] = Field(default_factory=dict)

    def event_level(self) -> Level:
        return Level(self.level)
