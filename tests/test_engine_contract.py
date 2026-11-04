"""MemoryEngine passes the engine contract."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import Relation, Schema
from pipeloom.testing.contract.engine import EngineContract, EngineHarness
from pipeloom.testing.memory import MemoryCatalog, MemoryEngine


class _Boom:
    def __iter__(self) -> Any:
        raise RuntimeError("cannot produce rows")

    def __len__(self) -> int:
        raise RuntimeError("cannot produce rows")


def harness(tmp_path: Path) -> EngineHarness:
    store: dict[str, Any] = {}

    def make(rows: Sequence[Mapping[str, Any]], schema: Schema) -> Any:
        return tuple(dict(r) for r in rows)

    def write_source(
        fmt: str, path: Path, rows: Sequence[Mapping[str, Any]], schema: Schema
    ) -> None:
        store[path.as_posix()] = tuple(dict(r) for r in rows)

    def failing(session: EngineSession) -> Relation:
        return Relation(_Boom(), "memory")

    return EngineHarness(
        engine=MemoryEngine(store),
        catalog=MemoryCatalog(store),
        make_native=make,
        to_rows=lambda native: [dict(r) for r in native],
        write_source=write_source,
        failing_relation=failing,
        settings_snapshot=dict,
    )


class TestMemoryEngine(EngineContract):
    @pytest.fixture
    def engine_factory(self) -> Callable[[Path], EngineHarness]:
        return harness
