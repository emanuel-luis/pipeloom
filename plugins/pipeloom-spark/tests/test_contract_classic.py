"""The Spark engine passes the engine contract on classic local Spark."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.protocols import Catalog
from pipeloom.testing.contract.engine import EngineContract, EngineHarness
from pipeloom_spark.catalog import DeltaPathCatalog, SparkSessionCatalog

pytestmark = [pytest.mark.spark, pytest.mark.timeout(300)]

Factory = Callable[[Any, Catalog, Path], EngineHarness]


class TestClassicDeltaPath(EngineContract):
    @pytest.fixture
    def engine_factory(
        self, classic_spark: Any, harness_factory: Factory
    ) -> Callable[[Path], EngineHarness]:
        return lambda tmp_path: harness_factory(
            classic_spark, DeltaPathCatalog(str(tmp_path / "wh")), tmp_path
        )


class TestClassicSparkCatalog(EngineContract):
    @pytest.fixture
    def engine_factory(
        self, classic_spark: Any, harness_factory: Factory
    ) -> Callable[[Path], EngineHarness]:
        return lambda tmp_path: harness_factory(classic_spark, SparkSessionCatalog(None), tmp_path)
