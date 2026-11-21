"""The context an engine session and a model receive at run time."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import TargetRef
from pipeloom.platform import Clock, Identity

if TYPE_CHECKING:
    from pipeloom.runtime.intervals import Backfill


@dataclass(frozen=True)
class RuntimeContext:
    run_id: str
    invocation_id: str
    project: str
    target: str
    project_dir: Path
    clock: Clock
    identity: Identity
    vars: Mapping[str, Any]
    full_refresh: bool = False
    orchestrator: Mapping[str, str] = field(default_factory=dict)
    run_started_at: datetime | None = None
    """Timezone-aware UTC; the same value in every invocation of a run, None only in a unit
    invocation that cannot find the run record."""
    artifacts_root: str = "target"
    """Absolute POSIX path or URI of the artifacts folder."""
    backfill: Backfill | None = None
    """Since 1.3: `pipeloom backfill` runs interval models over this range (ADR-0054)."""


@dataclass(frozen=True, slots=True)
class ModelContext:
    """What a Python model receives as `ctx`."""

    run_id: str
    invocation_id: str
    unit_id: str
    unique_id: str
    model: str
    attempt: int
    target: TargetRef | None
    materialized: str
    vars: Mapping[str, JsonValue]
    full_refresh: bool
    engine: EngineSession
    frame_api: Literal["native", "narwhals"]
    started_at: datetime
    is_incremental: bool = False
    watermark: Any = None
    lower_bound: Any = None
    """The last three are used from 0.3. Fields after them are wiring, not part of the API."""
    emit_log: Callable[[str, str, Mapping[str, JsonValue]], None] | None = field(
        default=None, repr=False, compare=False
    )
    _run_sql: Callable[[str, str | None, Mapping[str, Any] | None], Any] | None = field(
        default=None, repr=False, compare=False
    )
    """Private: set by the runner; backs `sql`."""
    interval: tuple[datetime, datetime] | None = None
    """Since 1.3: the UTC window `[start, end)` an interval model is running, else None. The
    incremental input is already filtered to it (ADR-0054)."""

    def sql(
        self, query: str, *, dialect: str | None = None, inputs: Mapping[str, Any] | None = None
    ) -> Any:
        """Run `query` over the model's inputs, named by alias as read (after the incremental
        filter). `inputs` adds or replaces frames by name. `dialect` (else the model's
        `sql_dialect`) is translated to the engine's when it differs. Returns the native frame,
        or a Narwhals frame under `frame_api: narwhals`."""
        if self._run_sql is None:
            raise RuntimeError("ctx.sql works only while pipeloom runs the model")
        return self._run_sql(query, dialect, inputs)

    def log(
        self,
        message: str,
        *,
        level: Literal["debug", "info", "warning"] = "info",
        **fields: JsonValue,
    ) -> None:
        """Emit a `model.log` event."""
        if self.emit_log is not None:
            self.emit_log(message, level, fields)
