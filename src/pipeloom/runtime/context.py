"""The context an engine session and a model receive at run time."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

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
