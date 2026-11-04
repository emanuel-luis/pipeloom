"""Protocols that engines, catalogs, naming strategies and secret providers implement."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from pipeloom.engine.types import (
    EngineCapabilities,
    Namespace,
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

    def filter_since(self, rel: Relation, column: str, lower: Any) -> Relation: ...

    def max_value(self, rel: Relation, column: str) -> Any: ...

    def cancel(self, unit_id: str) -> None:
        """Cooperative; a no-op for engines that cannot cancel."""
        ...


@runtime_checkable
class Engine(Protocol):
    name: str
    version: str
    capabilities: EngineCapabilities

    def session(self, ctx: RuntimeContext) -> AbstractContextManager[EngineSession]:
        """Open a session. Cleanup is guaranteed when the context exits."""
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
