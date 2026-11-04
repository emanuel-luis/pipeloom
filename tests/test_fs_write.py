"""Merge, replace_partitions and per-partition files on filesystem tables, on both engines."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.builtins.fs_table import FsCommit
from pipeloom.engine import portable
from pipeloom.engine.errors import CommitConflict, TransientError, WriteKeyError
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import Column, DeleteSpec, Schema, TargetRef, WriteConfig, WriteResult
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.clock import FakeClock

Rows = Sequence[Mapping[str, Any]]
KV = (Column("id", "int64"), Column("p", "string"), Column("v", "int64"))
TARGET = TargetRef(None, "main", "t")


def _polars_frame(rows: Rows, schema: Schema) -> Any:
    pl = pytest.importorskip("polars")
    from pipeloom_polars import io

    return pl.DataFrame(
        [dict(r) for r in rows], schema={c.name: io.polars_dtype(c.dtype) for c in schema}
    )


def _pandas_frame(rows: Rows, schema: Schema) -> Any:
    pd = pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    from pipeloom_pandas import io

    arrow = pa.schema([(c.name, io.arrow_type(c.dtype)) for c in schema])
    table = pa.Table.from_pylist([dict(r) for r in rows], schema=arrow)
    return table.to_pandas(types_mapper=pd.ArrowDtype)


def _rows(native: Any) -> list[dict[str, Any]]:
    frame = native.collect() if hasattr(native, "collect") else native
    if hasattr(frame, "to_dicts"):
        out = list(frame.to_dicts())
    else:
        pa = pytest.importorskip("pyarrow")
        out = pa.Table.from_pandas(frame, preserve_index=False).to_pylist()
    return sorted(out, key=lambda r: repr(sorted(r.items())))


class Env:
    def __init__(self, engine: str, root: Path, fmt: str) -> None:
        self.catalog = FilesystemCatalog(root, format=fmt, clock=FakeClock())
        if engine == "polars":
            from pipeloom_polars.engine import PolarsEngine

            self.engine: Any = PolarsEngine()
            self.make = _polars_frame
        else:
            from pipeloom_pandas.engine import PandasEngine

            self.engine = PandasEngine()
            self.make = _pandas_frame
        self.root = root

    @contextmanager
    def session(self) -> Iterator[EngineSession]:
        ctx = RuntimeContext(
            run_id="t",
            invocation_id="t",
            project="t",
            target="t",
            project_dir=self.root,
            clock=FakeClock(),
            identity=Identity("tester", "host"),
            vars={},
        )
        with self.engine.session(ctx, catalog=self.catalog) as s:
            yield s

    def put(
        self, s: EngineSession, rows: Rows, cfg: WriteConfig, token: str, schema: Schema = KV
    ) -> WriteResult:
        self.catalog.ensure_namespace(TARGET.namespace)
        rel = s.relation(self.make(rows, schema))
        return s.materializer(cfg.strategy).materialize(rel, TARGET, cfg, token)

    def table_rows(self, s: EngineSession) -> list[dict[str, Any]]:
        return _rows(s.reader("table").read(_spec()).native)

    def commit(self) -> FsCommit:
        c = self.catalog.table(TARGET).current()
        assert c is not None
        return c


def _spec() -> Any:
    from pipeloom.engine.types import ReadSpec

    return ReadSpec("table", table=TARGET)


@pytest.fixture(params=["polars", "pandas"])
def env_factory(request: pytest.FixtureRequest, tmp_path: Path) -> Callable[[str], Env]:
    pytest.importorskip(request.param)
    return lambda fmt="parquet": Env(request.param, tmp_path / "wh", fmt)


@pytest.fixture
def env(env_factory: Callable[[str], Env]) -> Env:
    return env_factory("parquet")


SEED = (
    {"id": 1, "p": "a", "v": 10},
    {"id": 2, "p": "a", "v": 20},
    {"id": 3, "p": "b", "v": 30},
    {"id": 4, "p": None, "v": 40},
)


def test_every_strategy_writes_one_file_per_partition(env: Env) -> None:
    with env.session() as s:
        env.put(s, SEED, WriteConfig("overwrite", partition_by=("p",)), "t1")
        c = env.commit()
        assert c.partition_by == ("p",) and len(c.files) == 3
        parts = {c.info(n).partition: c.info(n).rows for n in c.files}
        assert parts == {("a",): 2, ("b",): 1, (None,): 1}
        env.put(s, [{"id": 5, "p": "b", "v": 50}], WriteConfig("append", partition_by=("p",)), "t2")
        assert len(env.commit().files) == 4
        assert len(env.table_rows(s)) == 5


def test_replace_partitions_swaps_files_without_touching_the_others(env: Env) -> None:
    cfg = WriteConfig("replace_partitions", partition_by=("p",))
    with env.session() as s:
        env.put(s, SEED, WriteConfig("overwrite", partition_by=("p",)), "t1")
        before = env.commit()
        keep = [n for n in before.files if before.info(n).partition == ("b",)]
        result = env.put(
            s, [{"id": 9, "p": "a", "v": 90}, {"id": 8, "p": None, "v": 80}], cfg, "t2"
        )
        after = env.commit()
        assert len(keep) == 1 and set(keep) <= set(after.files) and len(after.files) == 3
        assert result.rows_deleted == 3 and after.rows == 3 and after.version == 2
        assert {r["id"] for r in env.table_rows(s)} == {3, 8, 9}


def test_replace_partitions_upgrades_a_v1_table(env: Env) -> None:
    cfg = WriteConfig("replace_partitions", partition_by=("p",))
    with env.session() as s:
        env.put(s, SEED, WriteConfig("overwrite"), "t1")
        path = env.catalog.table(TARGET).path / "_pipeloom" / "commit.json"
        doc = json.loads(path.read_text(encoding="utf-8"))
        doc["schema"] = "pipeloom.fs_table/1"
        del doc["file_info"], doc["partition_by"]
        path.write_text(json.dumps(doc), encoding="utf-8")
        assert env.commit().info(env.commit().files[0]).partition is None
        result = env.put(s, [{"id": 7, "p": "a", "v": 70}], cfg, "t2")
        assert result.rows_deleted == 2
        assert {r["id"] for r in env.table_rows(s)} == {3, 4, 7}
        assert json.loads(path.read_text(encoding="utf-8"))["schema"] == "pipeloom.fs_table/2"
        assert env.commit().rows == 3


def test_partition_values_round_trip(env: Env) -> None:
    schema = (
        Column("d", "date"),
        Column("t", "timestamp[us, UTC]"),
        Column("n", "decimal(10,2)"),
        Column("v", "int64"),
    )
    d, t, n = date(2026, 1, 2), datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC), Decimal("1.50")
    rows = [
        {"d": d, "t": t, "n": n, "v": 1},
        {"d": None, "t": None, "n": None, "v": 2},
        {"d": d, "t": t, "n": n, "v": 3},
    ]
    with env.session() as s:
        env.put(s, rows, WriteConfig("overwrite", partition_by=("d", "t", "n")), "t1", schema)
        c = env.commit()
        got = {
            tuple(
                portable.decode_partition_value(col.dtype, v)
                for col, v in zip(schema[:3], key, strict=True)
            )
            for key in (c.info(f).partition for f in c.files)
            if key is not None
        }
        assert got == {(d, t, n), (None, None, None)}
        batch = [{"d": d, "t": t, "n": n, "v": 9}, {"d": None, "t": None, "n": None, "v": 8}]
        cfg = WriteConfig("replace_partitions", partition_by=("d", "t", "n"))
        result = env.put(s, batch, cfg, "t2", schema)
        assert result.rows_deleted == 3
        assert [r["v"] for r in sorted(env.table_rows(s), key=lambda r: r["v"])] == [8, 9]


def test_empty_batches(env: Env) -> None:
    with env.session() as s:
        env.put(s, SEED, WriteConfig("overwrite", partition_by=("p",)), "t1")
        before = env.commit()
        merge = env.put(s, [], WriteConfig("merge", unique_key=("id",), partition_by=("p",)), "t2")
        mid = env.commit()
        assert mid.version == 2 and mid.files == before.files and merge.rows_written == 0
        repl = env.put(s, [], WriteConfig("replace_partitions", partition_by=("p",)), "t3")
        after = env.commit()
        assert after.version == 3 and after.files == before.files and repl.rows_deleted == 0
        assert s.applied_token(TARGET, "t3") is not None
        assert len(env.table_rows(s)) == 4


def test_merge_into_a_missing_table_inserts(env: Env) -> None:
    cfg = WriteConfig("merge", unique_key=("id",), deletes=DeleteSpec("p", "x", "hard"))
    with env.session() as s:
        result = env.put(s, [{"id": 1, "p": "a", "v": 1}, {"id": 2, "p": "x", "v": 2}], cfg, "t1")
        assert result.rows_inserted == 1 and env.commit().version == 1
        assert len(env.table_rows(s)) == 1


def test_a_batch_without_the_key_is_rejected(env: Env) -> None:
    schema = (Column("p", "string"), Column("v", "int64"))
    with env.session() as s:
        with pytest.raises(WriteKeyError):
            env.put(s, [{"p": "a", "v": 1}], WriteConfig("merge", unique_key=("id",)), "t1", schema)
        with pytest.raises(WriteKeyError):
            env.put(
                s,
                [{"p": "a", "v": 1}],
                WriteConfig("overwrite", partition_by=("zz",)),
                "t2",
                schema,
            )
        assert env.catalog.table(TARGET).current() is None


@pytest.mark.parametrize("fmt", ["csv", "ndjson"])
def test_merge_and_replace_on_text_formats(env_factory: Callable[[str], Env], fmt: str) -> None:
    env = env_factory(fmt)
    with env.session() as s:
        env.put(s, SEED, WriteConfig("overwrite", partition_by=("p",)), "t1")
        env.put(
            s,
            [{"id": 2, "p": "a", "v": 99}, {"id": 5, "p": "c", "v": 5}],
            WriteConfig("merge", unique_key=("id",)),
            "t2",
        )
        assert {r["id"]: r["v"] for r in env.table_rows(s)} == {1: 10, 2: 99, 3: 30, 4: 40, 5: 5}
        env.put(
            s,
            [{"id": 6, "p": "c", "v": 6}],
            WriteConfig("replace_partitions", partition_by=("p",)),
            "t3",
        )
        assert {r["id"] for r in env.table_rows(s)} == {1, 2, 3, 4, 6}


def test_two_threads_committing_one_table_conflict(env: Env) -> None:
    with env.session() as s:
        env.put(s, SEED, WriteConfig("overwrite"), "t1")
        gate = threading.Barrier(2)
        outcomes: list[BaseException | WriteResult] = []

        def work(i: int) -> None:
            cfg = WriteConfig("merge", unique_key=("id",))
            rel = s.relation(env.make([{"id": 10 + i, "p": "n", "v": i}], KV))
            mat = s.materializer("merge")
            try:
                outcomes.append(mat.materialize(rel, TARGET, cfg, f"t-{i}"))
            except BaseException as exc:  # noqa: BLE001 - inspected below
                outcomes.append(exc)

        # Every catalog.table() call returns a table whose commit waits for the other thread.
        original = env.catalog.table
        env.catalog.table = lambda target: _patched(original(target), gate)  # type: ignore[method-assign]
        threads = [threading.Thread(target=work, args=(i,)) for i in range(2)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        env.catalog.table = original  # type: ignore[method-assign]
        errors = [o for o in outcomes if isinstance(o, BaseException)]
        assert len(errors) == 1 and isinstance(errors[0], CommitConflict)
        assert isinstance(errors[0], TransientError)
        assert len(env.table_rows(s)) == 5 and env.commit().version == 2
        leftovers = list(env.catalog.table(TARGET).path.glob("part-*"))
        assert len(leftovers) == len(env.commit().files)


def _patched(table: Any, gate: threading.Barrier) -> Any:
    real = table.commit

    def commit(**kw: Any) -> Any:
        gate.wait(timeout=10)
        return real(**kw)

    table.commit = commit
    return table


def _ids(env: Env, s: EngineSession) -> list[int]:
    return sorted(r["v"] for r in env.table_rows(s))


def _check_parts(env: Env, total: int) -> None:
    c = env.commit()
    assert sum(c.info(n).rows or 0 for n in c.files) == total == c.rows


@pytest.mark.parametrize("strategy", ["append", "overwrite", "merge", "replace_partitions"])
def test_float_partitions_keep_every_row(env: Env, strategy: str) -> None:
    schema = (Column("f", "float64"), Column("v", "int64"))
    nan = float("nan")
    rows = [{"f": 1.0, "v": 1}, {"f": nan, "v": 2}, {"f": None, "v": 3}, {"f": 2.5, "v": 4}]
    cfg = WriteConfig(strategy, unique_key=("v",), partition_by=("f",))
    with env.session() as s:
        env.put(s, rows, cfg, "t1", schema)
        assert _ids(env, s) == [1, 2, 3, 4]
        _check_parts(env, 4)
        if strategy in ("merge", "replace_partitions"):
            batch = [{"f": nan, "v": 2}, {"f": 1.0, "v": 9}]
            env.put(s, batch, cfg, "t2", schema)
            want = [2, 4, 9] if strategy == "replace_partitions" else [1, 2, 3, 4, 9]
            assert _ids(env, s) == want
            _check_parts(env, len(want))


@pytest.mark.parametrize("strategy", ["append", "merge", "replace_partitions"])
def test_timestamp_partitions_with_zone(env: Env, strategy: str) -> None:
    schema = (Column("t", "timestamp[us, UTC]"), Column("v", "int64"))
    a, b = datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=UTC)
    rows = [{"t": a, "v": 1}, {"t": b, "v": 2}, {"t": None, "v": 3}, {"t": a, "v": 4}]
    cfg = WriteConfig(strategy, unique_key=("v",), partition_by=("t",))
    with env.session() as s:
        env.put(s, rows, WriteConfig("overwrite", partition_by=("t",)), "t1", schema)
        assert len(env.commit().files) == 3
        env.put(s, [{"t": a, "v": 9}], cfg, "t2", schema)
        want = {"append": 5, "merge": 5, "replace_partitions": 3}[strategy]
        assert len(_ids(env, s)) == want
        _check_parts(env, want)


def test_nanosecond_partitions(env: Env) -> None:
    ns = [1767225600000000001, 1767225600000000002, 1767225600000000001]
    if env.engine.name == "polars":
        import polars as pl

        frame: Any = pl.DataFrame({"t": pl.Series(ns).cast(pl.Datetime("ns")), "v": [1, 2, 3]})
    else:
        import pandas as pd

        frame = pd.DataFrame({"t": pd.to_datetime(ns, unit="ns"), "v": [1, 2, 3]})
    env.catalog.ensure_namespace(TARGET.namespace)
    with env.session() as s:
        for strategy, token in (("overwrite", "t1"), ("replace_partitions", "t2")):
            cfg = WriteConfig(strategy, partition_by=("t",))
            s.materializer(strategy).materialize(s.relation(frame), TARGET, cfg, token)
            assert _ids(env, s) == [1, 2, 3]
            _check_parts(env, 3)
        assert len(env.commit().files) == 2


def test_decimal_partitions_match_after_a_scale_widening(env: Env) -> None:
    narrow = (Column("n", "decimal(10,2)"), Column("v", "int64"))
    wide = (Column("n", "decimal(12,3)"), Column("v", "int64"))
    with env.session() as s:
        env.put(
            s,
            [{"n": Decimal("1.50"), "v": 1}],
            WriteConfig("overwrite", partition_by=("n",)),
            "t1",
            narrow,
        )
        cfg = WriteConfig("replace_partitions", partition_by=("n",), columns=wide)
        result = env.put(s, [{"n": Decimal("1.500"), "v": 2}], cfg, "t2", wide)
        assert result.rows_deleted == 1 and _ids(env, s) == [2]
        _check_parts(env, 1)


@pytest.mark.parametrize("unit", ["ns", "us", "ms", "s"])
@pytest.mark.parametrize("strategy", ["append", "merge", "replace_partitions"])
def test_missing_timestamps_are_one_null_partition(env: Env, unit: str, strategy: str) -> None:
    """A pandas NaT must stay null, not become the integer it casts to."""
    stamps = ["2026-01-01", None, "2026-01-02", None]
    if env.engine.name == "polars":
        if unit == "s":
            pytest.skip("polars has no second unit")
        import polars as pl

        def make(vals: list[Any], ids: list[int]) -> Any:
            col = pl.Series([None if v is None else datetime.fromisoformat(v) for v in vals])
            return pl.DataFrame({"t": col.cast(pl.Datetime(unit)), "v": ids})  # type: ignore[arg-type]
    else:
        import pandas as pd

        def make(vals: list[Any], ids: list[int]) -> Any:
            return pd.DataFrame({"t": pd.to_datetime(vals).astype(f"datetime64[{unit}]"), "v": ids})

    env.catalog.ensure_namespace(TARGET.namespace)
    with env.session() as s:
        first = WriteConfig("overwrite", partition_by=("t",))
        s.materializer("overwrite").materialize(
            s.relation(make(stamps, [1, 2, 3, 4])), TARGET, first, "t1"
        )
        c = env.commit()
        assert sorted(str(c.info(n).partition) for n in c.files) == sorted(
            ["('2026-01-01T00:00:00.000000000',)", "('2026-01-02T00:00:00.000000000',)", "(None,)"]
        )
        assert _ids(env, s) == [1, 2, 3, 4]
        cfg = WriteConfig(strategy, unique_key=("v",), partition_by=("t",))
        batch = make([None, "2026-01-02"], [2, 9])
        s.materializer(strategy).materialize(s.relation(batch), TARGET, cfg, "t2")
        want = {
            "append": [1, 2, 2, 3, 4, 9],
            "merge": [1, 2, 3, 4, 9],
            "replace_partitions": [1, 2, 9],
        }
        assert _ids(env, s) == want[strategy]
        _check_parts(env, len(want[strategy]))
