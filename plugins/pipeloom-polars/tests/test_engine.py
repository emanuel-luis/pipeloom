"""Polars engine details beyond the contract."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.engine.types import ReadSpec, TargetRef, WriteConfig
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import MemoryCatalog
from pipeloom_polars.engine import PolarsEngine

T = TargetRef(None, "main", "t")


def ctx(tmp_path: Path) -> RuntimeContext:
    return RuntimeContext("r", "i", "p", "t", tmp_path, FakeClock(), Identity("u", "h"), {})


@pytest.mark.parametrize("fmt", ["parquet", "csv", "ndjson"])
def test_tables_round_trip_in_every_catalog_format(tmp_path: Path, fmt: str) -> None:
    catalog = FilesystemCatalog(tmp_path / "wh", format=fmt)
    with PolarsEngine().session(ctx(tmp_path), catalog=catalog) as s:
        catalog.ensure_namespace(T.namespace)
        rel = s.relation(pl.DataFrame({"id": [1, 2], "label": ["a", None]}))
        s.materializer("overwrite").materialize(rel, T, WriteConfig("overwrite"), "r:1")
        out = s.reader("table").read(ReadSpec("table", table=T))
        assert out.native.collect().sort("id").to_dicts() == [
            {"id": 1, "label": "a"},
            {"id": 2, "label": None},
        ]
        assert [c.dtype for c in s.schema(out)] == ["int64", "string"]
    assert len(list((tmp_path / "wh" / "main" / "t").glob(f"part-*.{fmt}"))) == 1


def test_a_replayed_token_keeps_the_commit(tmp_path: Path) -> None:
    catalog = FilesystemCatalog(tmp_path / "wh")
    with PolarsEngine().session(ctx(tmp_path), catalog=catalog) as s:
        catalog.ensure_namespace(T.namespace)
        rel = s.relation(pl.DataFrame({"id": [1]}))
        mat = s.materializer("append")
        first = mat.materialize(rel, T, WriteConfig("append"), "r:1")
        before = sorted(p.name for p in (tmp_path / "wh" / "main" / "t").iterdir())
        assert mat.materialize(rel, T, WriteConfig("append"), "r:1") == first
        assert sorted(p.name for p in (tmp_path / "wh" / "main" / "t").iterdir()) == before
        assert first.rows_written == 1 and first.bytes_written and first.target_version == 1


def test_the_session_needs_the_filesystem_catalog(tmp_path: Path) -> None:
    with (
        pytest.raises(TypeError, match="filesystem catalog"),
        PolarsEngine().session(ctx(tmp_path), catalog=MemoryCatalog()),
    ):
        pass


def test_unsupported_format_and_strategy(tmp_path: Path) -> None:
    catalog = FilesystemCatalog(tmp_path / "wh")
    with PolarsEngine().session(ctx(tmp_path), catalog=catalog) as s:
        with pytest.raises(ValueError, match="format"):
            s.reader("orc")
        with pytest.raises(ValueError, match="strategy"):
            s.materializer("merge")


def test_a_missing_table_raises(tmp_path: Path) -> None:
    catalog = FilesystemCatalog(tmp_path / "wh")
    with (
        PolarsEngine().session(ctx(tmp_path), catalog=catalog) as s,
        pytest.raises(FileNotFoundError),
    ):
        s.reader("table").read(ReadSpec("table", table=T))
