"""The Spark leg of the cross-engine matrix (tests/portable/test_matrix.py): the same portable
model and input rows give the same rows as on polars, pandas and DuckDB."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tests.portable.matrix import EXPECTED, open_session, run_model

from pipeloom.testing.reference import sort_key
from pipeloom_spark.catalog import DeltaPathCatalog
from pipeloom_spark.engine import SparkEngine
from pipeloom_spark.spec import SparkEngineOptions

pytestmark = [pytest.mark.spark, pytest.mark.timeout(300)]


def test_portable_model_rows(classic_spark: Any, tmp_path: Path) -> None:
    engine = SparkEngine(SparkEngineOptions(mode="active"))
    with open_session(engine, DeltaPathCatalog(str(tmp_path / "wh")), tmp_path) as session:
        assert run_model(session) == sorted(EXPECTED, key=sort_key)
