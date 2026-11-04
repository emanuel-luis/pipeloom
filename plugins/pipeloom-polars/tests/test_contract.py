"""The polars engine passes the engine contract."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import Relation, Schema
from pipeloom.testing.contract.engine import EngineContract, EngineHarness
from pipeloom_polars import io
from pipeloom_polars.engine import PolarsEngine

Rows = Sequence[Mapping[str, Any]]


def _frame(rows: Rows, schema: Schema) -> pl.DataFrame:
    dtypes = {c.name: io.polars_dtype(c.dtype) for c in schema}
    return pl.DataFrame([dict(r) for r in rows], schema=dtypes)


def _boom(df: pl.DataFrame) -> pl.DataFrame:
    raise RuntimeError("cannot produce rows")


def harness(tmp_path: Path) -> EngineHarness:
    def write_source(fmt: str, path: Path, rows: Rows, schema: Schema) -> None:
        df = _frame(rows, schema)
        if fmt == "parquet":
            df.write_parquet(path)
        elif fmt == "csv":
            df.write_csv(path)
        elif fmt == "ndjson":
            df.write_ndjson(path)
        else:
            path.write_text(df.write_json(), encoding="utf-8")

    def to_rows(native: Any) -> list[dict[str, Any]]:
        df = native.collect() if isinstance(native, pl.LazyFrame) else native
        return list(df.to_dicts())

    def failing(session: EngineSession) -> Relation:
        return session.relation(pl.LazyFrame({"a": [1]}).map_batches(_boom))

    return EngineHarness(
        engine=PolarsEngine(),
        catalog=FilesystemCatalog(tmp_path / "wh"),
        make_native=lambda rows, schema: _frame(rows, schema).lazy(),
        to_rows=to_rows,
        write_source=write_source,
        failing_relation=failing,
        settings_snapshot=lambda: pl.Config.state(),
    )


class TestPolarsEngine(EngineContract):
    @pytest.fixture
    def engine_factory(self) -> Callable[[Path], EngineHarness]:
        return harness
