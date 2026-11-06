"""The plan: what a compile produces and what a run consumes. Frozen pydantic models."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from pipeloom.config.models import ColumnConfig, ResolvedModelConfig
from pipeloom.engine.types import TargetRef


def default_strategy(materialized: str) -> str:
    """The write strategy a persisted model uses when it sets none."""
    # Mirrors validate.semantics.effective_strategy: validate sits below plan in the layers.
    return "append" if materialized == "incremental" else "overwrite"


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", protected_namespaces=())


class SqlBody(_Frozen):
    kind: Literal["sql"] = "sql"
    sql: str


class PythonBody(_Frozen):
    kind: Literal["python"] = "python"
    callable: str
    """`module:function`."""
    file: str
    """The module's file, POSIX and relative to the project dir."""


Body = Annotated[SqlBody | PythonBody, Field(discriminator="kind")]


class PlanNode(_Frozen):
    unique_id: str
    name: str
    file: str
    line: int | None
    materialized: str
    body: Body
    inputs: dict[str, str]
    """Alias to the unique id of the model or source it reads."""
    relation: TargetRef | None
    """Where the model is written; None for ephemeral models."""
    unit_ids: tuple[str, ...]
    """Units that run this node: its own, or every unit that inlines it."""
    config: ResolvedModelConfig
    tags: tuple[str, ...]
    checksum: str
    columns: tuple[ColumnConfig, ...] | None = None
    """The model contract; the manifest leaves the key out when the model declares none."""


class PlanSource(_Frozen):
    unique_id: str
    name: str
    file: str
    format: str | None
    location: str | None
    table: str | None
    options: dict[str, Any]
    tags: tuple[str, ...]


class ExecutionUnit(_Frozen):
    unit_id: str
    """The name of the persisted model the unit produces."""
    nodes: tuple[str, ...]
    """Unique ids in run order: inlined ephemeral models first, the persisted model last."""
    parents: tuple[str, ...]
    """Unit ids this unit reads from."""


class PluginInfo(_Frozen):
    kind: str
    distribution: str
    version: str


class EnvVar(_Frozen):
    name: str
    default_used: bool


class PlanMetadata(_Frozen):
    pipeloom_version: str
    plugins: dict[str, PluginInfo]
    config_version: int
    plan_hash: str
    project: str
    target: str
    boundary: Literal["process", "task"]
    python_paths: tuple[str, ...]
    vars: dict[str, Any]
    env_vars: tuple[EnvVar, ...]
    generated_at: str
    invocation_id: str
    deploy: dict[str, Any] | None = None
    """The target's validated deploy options, `kind` included. Not part of the plan hash."""


class PlanSelection(_Frozen):
    select: tuple[str, ...]
    exclude: tuple[str, ...]
    selected: tuple[str, ...]
    """Unique ids of the selected models, sorted."""


class Plan(_Frozen):
    metadata: PlanMetadata
    nodes: dict[str, PlanNode]
    sources: dict[str, PlanSource]
    units: dict[str, ExecutionUnit]
    parent_map: dict[str, tuple[str, ...]]
    child_map: dict[str, tuple[str, ...]]
    selection: PlanSelection
