"""Protocols that engines, catalogs, naming strategies and secret providers implement."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pipeloom.engine.types import (
    DropResult,
    EngineCapabilities,
    ExpectationSpec,
    IntermediateRef,
    Namespace,
    QualityCounts,
    QuarantineMeta,
    ReadSpec,
    Relation,
    Schema,
    TargetRef,
    WriteConfig,
    WriteResult,
)
from pipeloom.platform import Identity

if TYPE_CHECKING:
    from pipeloom.runtime.context import RuntimeContext


@runtime_checkable
class Reader(Protocol):
    def read(self, spec: ReadSpec) -> Relation: ...


@runtime_checkable
class Materializer(Protocol):
    """One per write strategy."""

    strategy: str

    def materialize(
        self, rel: Relation, target: TargetRef, cfg: WriteConfig, token: str
    ) -> WriteResult:
        """Write `rel`. `token` is the idempotency key: repeating it must not write twice."""
        ...


@runtime_checkable
class EngineSession(Protocol):
    native: Any
    """Escape hatch to the engine's own session object."""

    def reader(self, fmt: str) -> Reader: ...

    def materializer(self, strategy: str) -> Materializer: ...

    def sql(self, query: str, inputs: Mapping[str, Relation]) -> Relation: ...

    def schema(self, rel: Relation) -> Schema: ...

    def count(self, rel: Relation) -> int: ...

    def filter_since(
        self, rel: Relation, column: str, lower: Any, *, inclusive: bool = True
    ) -> Relation:
        """Rows with `column >= lower` (`>` when not inclusive); null rows are dropped."""
        ...

    def relation(self, native: Any) -> Relation:
        """Check a model's return value and wrap it; raise TypeError for a foreign type."""
        ...

    def persist(self, rel: Relation) -> Relation:
        """An in-memory copy that later reads reuse, for results shared by several units."""
        ...

    def create_view(
        self, target: TargetRef, query: str, inputs: Mapping[str, ReadSpec]
    ) -> WriteResult:
        """Create or replace a catalog view over `inputs`, which `query` names by alias."""
        ...

    def write_intermediate(self, rel: Relation, ref: IntermediateRef, token: str) -> WriteResult:
        """Persist `rel` at `ref.location`, overwriting; repeating `token` must not write twice.
        Needs the `intermediate` materialization."""
        ...

    def max_value(self, rel: Relation, column: str) -> Any:
        """The maximum as a plain Python value (never numpy or pandas); None when empty."""
        ...

    def evaluate(self, rel: Relation, checks: Sequence[ExpectationSpec]) -> QualityCounts:
        """Raw counts and aggregate values; the core judges them. Needs `quality`."""
        ...

    def apply_drops(
        self, rel: Relation, checks: Sequence[ExpectationSpec], *, quarantine: QuarantineMeta | None
    ) -> DropResult:
        """Split `rel` by the drop checks. Needs `quality`."""
        ...

    def align(self, rel: Relation, schema: Schema) -> Relation:
        """Select in `schema` order, add missing columns as typed nulls, cast to the types."""
        ...

    def applied_token(self, target: TargetRef, token: str) -> WriteResult | None:
        """The stored result (with `skipped=True`) of a write that used `token`, else None.
        None also when the engine is not idempotent."""
        ...

    def unit_scope(self, unit_id: str) -> AbstractContextManager[None]:
        """Tags the work done in this thread with `unit_id`, so `cancel` can find it."""
        ...

    def is_transient(self, exc: BaseException) -> bool | None:
        """True for a retryable error, False for a permanent one, None for no opinion."""
        ...

    def cancel(self, unit_id: str) -> None:
        """Cooperative; a no-op for engines that cannot cancel."""
        ...


@runtime_checkable
class Engine(Protocol):
    name: str
    version: str
    capabilities: EngineCapabilities

    def session(
        self, ctx: RuntimeContext, *, catalog: Catalog
    ) -> AbstractContextManager[EngineSession]:
        """Open a session, one per invocation; it must be thread-safe. Cleanup is guaranteed
        when the context exits."""
        ...


@runtime_checkable
class Catalog(Protocol):
    def exists(self, target: TargetRef) -> bool: ...

    def ensure_namespace(self, ns: Namespace) -> None:
        """Idempotent."""
        ...

    def table_schema(self, target: TargetRef) -> Schema | None: ...


@runtime_checkable
class SecretProvider(Protocol):
    def resolve(self, path: str) -> str: ...


@runtime_checkable
class NamingStrategy(Protocol):
    def relation(self, name: str, schema: str | None, identity: Identity) -> TargetRef:
        """The physical relation for a model. `schema` is the model's own, if it sets one."""
        ...
