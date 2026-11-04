"""The context an engine session and a model receive at run time."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pipeloom.diagnostics import JsonValue
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import TargetRef
from pipeloom.platform import Clock, Identity


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
    """The last three are used from 0.3."""
    emit_log: Callable[[str, str, Mapping[str, JsonValue]], None] | None = field(
        default=None, repr=False, compare=False
    )

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
