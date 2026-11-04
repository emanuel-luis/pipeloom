"""The pandas engine passes the engine contract."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import Relation, Schema
from pipeloom.testing.contract.engine import EngineContract, EngineHarness
from pipeloom_pandas import io
from pipeloom_pandas.engine import PandasEngine

Rows = Sequence[Mapping[str, Any]]


def _frame(rows: Rows, schema: Schema) -> pd.DataFrame:
    arrow = pa.schema([(c.name, io.arrow_type(c.dtype)) for c in schema])
    table = pa.Table.from_pylist([dict(r) for r in rows], schema=arrow)
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def harness(tmp_path: Path) -> EngineHarness:
    def write_source(fmt: str, path: Path, rows: Rows, schema: Schema) -> None:
        df = _frame(rows, schema)
        if fmt == "json":
            df.to_json(path, orient="records")
        else:
            io.write(df, fmt, path)

    def to_rows(native: Any) -> list[dict[str, Any]]:
        table = pa.Table.from_pandas(native, preserve_index=False)
        return list(table.to_pylist())

    def failing(session: EngineSession) -> Relation:
        return session.relation(pd.DataFrame({"a": [object()]}))  # parquet cannot store it

    return EngineHarness(
        engine=PandasEngine(),
        catalog=FilesystemCatalog(tmp_path / "wh"),
        make_native=_frame,
        update_condition="source.v > target.v",
        to_rows=to_rows,
        write_source=write_source,
        failing_relation=failing,
        settings_snapshot=lambda: {
            "string_storage": pd.get_option("mode.string_storage"),
            "max_rows": pd.get_option("display.max_rows"),
        },
    )


class TestPandasEngine(EngineContract):
    @pytest.fixture
    def engine_factory(self) -> Callable[[Path], EngineHarness]:
        return harness
