"""The cross-engine matrix: one portable model gives the same rows on polars, pandas and DuckDB.

The Spark leg lives in plugins/pipeloom-spark/tests/test_portable_matrix.py, where the Spark
session and its PIPELOOM_TEST_SPARK gate already are.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path

import pytest

from pipeloom.engine.protocols import EngineSession
from pipeloom.testing.reference import sort_key
from tests.portable.matrix import EXPECTED, SessionOpener, open_session, run_model


def _polars(tmp_path: Path) -> AbstractContextManager[EngineSession]:
    from pipeloom.builtins.filesystem import FilesystemCatalog
    from pipeloom_polars.engine import PolarsEngine

    return open_session(PolarsEngine(), FilesystemCatalog(tmp_path / "wh"), tmp_path)


def _pandas(tmp_path: Path) -> AbstractContextManager[EngineSession]:
    from pipeloom.builtins.filesystem import FilesystemCatalog
    from pipeloom_pandas.engine import PandasEngine

    return open_session(PandasEngine(), FilesystemCatalog(tmp_path / "wh"), tmp_path)


def _duckdb(tmp_path: Path) -> AbstractContextManager[EngineSession]:
    from pipeloom_duckdb.catalog import DuckDBCatalog
    from pipeloom_duckdb.engine import DuckDBEngine

    catalog = DuckDBCatalog((tmp_path / "wh" / "warehouse.duckdb").as_posix())
    return open_session(DuckDBEngine(), catalog, tmp_path)


@pytest.mark.parametrize(
    ("module", "opener"),
    [("pipeloom_polars", _polars), ("pipeloom_pandas", _pandas), ("pipeloom_duckdb", _duckdb)],
)
def test_portable_model_rows(module: str, opener: SessionOpener, tmp_path: Path) -> None:
    pytest.importorskip(module)
    with opener(tmp_path) as session:
        assert run_model(session) == sorted(EXPECTED, key=sort_key)
