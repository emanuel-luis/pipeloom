"""The Spark engine passes the engine contract through a local Spark Connect server."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.protocols import Catalog
from pipeloom.testing.contract.engine import EngineContract, EngineHarness
from pipeloom_spark.catalog import SparkSessionCatalog

pytestmark = [pytest.mark.spark_connect, pytest.mark.timeout(300)]

Factory = Callable[[Any, Catalog, Path], EngineHarness]


class TestConnectSparkCatalog(EngineContract):
    @pytest.fixture
    def engine_factory(
        self, connect_spark: Any, harness_factory: Factory
    ) -> Callable[[Path], EngineHarness]:
        return lambda tmp_path: harness_factory(connect_spark, SparkSessionCatalog(None), tmp_path)
