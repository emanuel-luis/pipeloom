"""pandas engine details beyond the contract."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.engine import portable
from pipeloom.engine.types import ReadSpec, TargetRef, WriteConfig
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import MemoryCatalog
from pipeloom_pandas.engine import PandasEngine

T = TargetRef(None, "main", "t")


def ctx(tmp_path: Path) -> RuntimeContext:
    return RuntimeContext("r", "i", "p", "t", tmp_path, FakeClock(), Identity("u", "h"), {})


@pytest.mark.parametrize("fmt", ["parquet", "csv", "ndjson"])
def test_tables_round_trip_in_every_catalog_format(tmp_path: Path, fmt: str) -> None:
    catalog = FilesystemCatalog(tmp_path / "wh", format=fmt)
    with PandasEngine().session(ctx(tmp_path), catalog=catalog) as s:
        catalog.ensure_namespace(T.namespace)
        rel = s.relation(pd.DataFrame({"id": [1, 2], "label": ["a", None]}))
        s.materializer("overwrite").materialize(rel, T, WriteConfig("overwrite"), "r:1")
        out = s.reader("table").read(ReadSpec("table", table=T))
        frame = out.native.sort_values("id")
        assert [(r.id, None if pd.isna(r.label) else r.label) for r in frame.itertuples()] == [
            (1, "a"),
            (2, None),
        ]
        assert [c.dtype for c in s.schema(out)] == ["int64", "string"]
    assert len(list((tmp_path / "wh" / "main" / "t").glob(f"part-*.{fmt}"))) == 1


def test_a_replayed_token_keeps_the_commit(tmp_path: Path) -> None:
    catalog = FilesystemCatalog(tmp_path / "wh")
    with PandasEngine().session(ctx(tmp_path), catalog=catalog) as s:
        catalog.ensure_namespace(T.namespace)
        rel = s.relation(pd.DataFrame({"id": [1]}))
        mat = s.materializer("append")
        first = mat.materialize(rel, T, WriteConfig("append"), "r:1")
        before = sorted(p.name for p in (tmp_path / "wh" / "main" / "t").iterdir())
        assert (
            replace(mat.materialize(rel, T, WriteConfig("append"), "r:1"), skipped=False) == first
        )
        assert sorted(p.name for p in (tmp_path / "wh" / "main" / "t").iterdir()) == before
        assert first.rows_written == 1 and first.bytes_written and first.target_version == 1


def test_the_session_needs_the_filesystem_catalog(tmp_path: Path) -> None:
    with (
        pytest.raises(TypeError, match="filesystem catalog"),
        PandasEngine().session(ctx(tmp_path), catalog=MemoryCatalog()),
    ):
        pass


def test_unsupported_format_strategy_sql_and_view(tmp_path: Path) -> None:
    catalog = FilesystemCatalog(tmp_path / "wh")
    with PandasEngine().session(ctx(tmp_path), catalog=catalog) as s:
        with pytest.raises(ValueError, match="format"):
            s.reader("orc")
        with pytest.raises(ValueError, match="strategy"):
            s.materializer("merge")
        with pytest.raises(NotImplementedError):
            s.sql("select 1", {})
        with pytest.raises(NotImplementedError):
            s.create_view(T, "select 1", {})


def test_a_missing_table_raises(tmp_path: Path) -> None:
    catalog = FilesystemCatalog(tmp_path / "wh")
    with (
        PandasEngine().session(ctx(tmp_path), catalog=catalog) as s,
        pytest.raises(FileNotFoundError),
    ):
        s.reader("table").read(ReadSpec("table", table=T))


def test_string_columns_map_to_string_in_either_pandas_generation() -> None:
    df = pd.DataFrame({"a": ["x", "y"], "b": pd.array(["x", None], dtype="string")})
    assert [(c.name, c.dtype) for c in portable.schema(df)] == [("a", "string"), ("b", "string")]


PANDAS_PROJECT = {
    "pipeloom.yml": (
        "config_version: 1\nname: p\npython_paths: [src]\n"
        "targets:\n  dev:\n    engine: {kind: pandas}\n"
        "    catalog: {kind: filesystem, root: wh}\n"
    ),
}


def codes(tmp_path: Path, model: str) -> set[str]:
    files = {**PANDAS_PROJECT, "models/m.yml": f"models:\n  - {model}\n"}
    for rel, content in files.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_bytes(content.encode("utf-8"))
    root = tmp_path
    outcome = Project(ProjectOptions(project_dir=root, target="dev", env={})).validate()
    return {d.code for d in outcome.diagnostics}


def test_a_sql_model_on_pandas_is_plc502(tmp_path: Path) -> None:
    assert "PLC502" in codes(tmp_path, "{name: a, sql: select 1}")


def test_a_view_on_pandas_is_plc507(tmp_path: Path) -> None:
    assert "PLC507" in codes(tmp_path, "{name: a, sql: select 1, materialized: view}")


@pytest.mark.parametrize("fmt", ["csv", "ndjson"])
def test_text_formats_restore_each_stored_type_even_beside_an_unknown_one(
    tmp_path: Path, fmt: str
) -> None:
    from datetime import UTC, date, datetime
    from decimal import Decimal

    import pyarrow as pa

    arrow = pa.table(
        {
            "d": pa.array([date(2026, 1, 2), None], pa.date32()),
            "t": pa.array(
                [datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC), None], pa.timestamp("us", "UTC")
            ),
            "n": pa.array([Decimal("12.34"), None], pa.decimal128(18, 2)),
            "i": pa.array([1, None], pa.int64()),
            "z": pa.array(["007", None], pa.string()),
            "us": pa.array([datetime(2026, 1, 2, 3, 4, 5, 123456), None], pa.timestamp("us")),
            "u": pa.array([[1], None], pa.list_(pa.int64())),  # a type the engine cannot name
        }
    )
    catalog = FilesystemCatalog(tmp_path / "wh", format=fmt)
    with PandasEngine().session(ctx(tmp_path), catalog=catalog) as s:
        catalog.ensure_namespace(T.namespace)
        rel = s.relation(arrow.to_pandas(types_mapper=pd.ArrowDtype))
        s.materializer("overwrite").materialize(rel, T, WriteConfig("overwrite"), "r:1")
        out = s.reader("table").read(ReadSpec("table", table=T))
        got = {c.name: c.dtype for c in s.schema(out)}
        want = {c.name: c.dtype for c in s.schema(rel)}
        keys = ["d", "t", "n", "i", "z", "us"]
        assert {k: got[k] for k in keys} == {k: want[k] for k in keys}
        assert out.native["z"].iloc[0] == "007"
        assert out.native["us"].iloc[0] == pd.Timestamp("2026-01-02 03:04:05.123456")
        assert out.native["t"].iloc[0] == pd.Timestamp("2026-01-02 03:04:05", tz="UTC")
        assert out.native["n"].iloc[0] == Decimal("12.34")
