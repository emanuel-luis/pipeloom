"""The plan: what a compile produces and what a run consumes. Frozen pydantic models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from pipeloom.config.models import ColumnConfig, DataTestConfig, ResolvedModelConfig
from pipeloom.engine.types import TargetRef


def default_strategy(materialized: str) -> str:
    """The write strategy a persisted model uses when it sets none."""
    # Mirrors validate.semantics.effective_strategy: validate sits below plan in the layers.
    return "append" if materialized == "incremental" else "overwrite"


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", protected_namespaces=())


class PythonBody(_Frozen):
    """A model body. Manifests from before 0.9 may hold `kind: sql`, which no longer loads
    (PLX107)."""

    kind: Literal["python"] = "python"
    callable: str
    """`module:function`."""
    file: str
    """The module's file, POSIX and relative to the project dir."""


class SqlQuery(_Frozen):
    sql: str
    """The query text as written."""
    dialect: str | None = None
    """The call's literal `dialect=`; None when the call sets none."""


class PlanSql(_Frozen):
    """The literal `ctx.sql("...")` queries of a model's function (ADR-0052), for `plan --diff`."""

    shell: str
    """The code digest with these query strings blanked."""
    queries: tuple[SqlQuery, ...]
    returned: bool = False
    """True when the function has exactly one literal query, called with at most `dialect=`,
    and returns that call as it is. Only then can a query change be non-breaking."""


class PlanNode(_Frozen):
    unique_id: str
    name: str
    file: str
    line: int | None
    materialized: str
    body: PythonBody
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
    data_tests: tuple[DataTestConfig, ...] | None = None
    """Post-write checks; the manifest leaves the key out when the model declares none."""
    checksums: dict[str, str] | None = None
    """Split checksums (ADR-0052): `code`, `config`, `contract`, `tests` and `upstream`. Absent in
    1.0 manifests; never part of the plan hash."""
    checksum_version: int | None = None
    """How `checksums` was computed; absent with it."""
    sql: PlanSql | None = None
    """Absent unless the model's function passes a string literal to `ctx.sql`. Never part of
    the plan hash."""


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
