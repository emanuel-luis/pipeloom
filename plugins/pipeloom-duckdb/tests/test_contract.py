"""The DuckDB engine passes the engine contract."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from pipeloom.testing.contract.engine import EngineContract, EngineHarness


class TestDuckDBEngine(EngineContract):
    @pytest.fixture
    def engine_factory(
        self, duckdb_harness: Callable[[Path], EngineHarness]
    ) -> Callable[[Path], EngineHarness]:
        return duckdb_harness
