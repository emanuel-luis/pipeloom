"""Delta write behavior beyond the contract, on classic local Spark (delta_path unless a test
names spark_catalog)."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.errors import WriteRefusedError
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import Column, DeleteSpec, ReadSpec, TargetRef, WriteConfig
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.clock import FakeClock
from pipeloom_spark import delta
from pipeloom_spark.catalog import DeltaPathCatalog, SparkSessionCatalog
from pipeloom_spark.engine import SparkEngine
from pipeloom_spark.spec import SparkEngineOptions

pytestmark = [pytest.mark.spark, pytest.mark.timeout(300)]

T0 = datetime(2026, 1, 1, tzinfo=UTC)
T1 = datetime(2026, 1, 2, tzinfo=UTC)


def _target(name: str | TargetRef) -> TargetRef:
    return name if isinstance(name, TargetRef) else TargetRef(None, "main", name)


@contextmanager
def _open(
    tmp_path: Path, kind: str = "delta_path"
) -> Iterator[tuple[EngineSession, DeltaPathCatalog | SparkSessionCatalog]]:
    """A session on `kind`; on `spark_catalog`, tables go to `default` under unique names."""
    catalog: DeltaPathCatalog | SparkSessionCatalog = (
        DeltaPathCatalog(str(tmp_path / "wh"))
        if kind == "delta_path"
        else SparkSessionCatalog(None)
    )
    ctx = RuntimeContext(
        run_id="r1",
        invocation_id="i1",
        project="shop",
        target="t",
        project_dir=tmp_path,
        clock=FakeClock(),
        identity=Identity("tester", "host"),
        vars={},
    )
    with SparkEngine(SparkEngineOptions(mode="active")).session(ctx, catalog=catalog) as s:
        yield s, catalog


def _unique(kind: str) -> str | TargetRef:
    """`m` on delta_path (a fresh root per test); a fresh `default` table on spark_catalog."""
    return "m" if kind == "delta_path" else TargetRef(None, "default", f"m_{uuid.uuid4().hex}")


def _write(
    s: EngineSession,
    spark: Any,
    name: str | TargetRef,
    rows: list[tuple[Any, ...]],
    ddl: str,
    cfg: WriteConfig,
    token: str,
) -> Any:
    frame = s.relation(spark.createDataFrame(rows, ddl))
    return s.materializer(cfg.strategy).materialize(frame, _target(name), cfg, token)


def _rows(s: EngineSession, name: str | TargetRef, order: str) -> list[tuple[Any, ...]]:
    frame = s.reader("table").read(ReadSpec("table", table=_target(name))).native
    return [
        tuple(v.astimezone(UTC) if isinstance(v, datetime) else v for v in r)
        for r in frame.orderBy(order).collect()
    ]


_REFUSED = {
    "append": WriteConfig("append", run_started_at=T0),
    "overwrite": WriteConfig("overwrite", run_started_at=T0),
    "replace_partitions": WriteConfig(
        "replace_partitions", partition_by=("id",), run_started_at=T0
    ),
    "merge": WriteConfig("merge", unique_key=("id",), run_started_at=T0),
    # the dropped column would run a migration commit before the write
    "append_migration": WriteConfig("append", columns=(Column("id", "int64"),), run_started_at=T0),
    "merge_migration": WriteConfig(
        "merge", unique_key=("id",), columns=(Column("id", "int64"),), run_started_at=T0
    ),
}


@pytest.mark.parametrize("kind", ["delta_path", "spark_catalog"])
@pytest.mark.parametrize("case", sorted(_REFUSED))
def test_a_later_transaction_version_makes_an_earlier_run_fail_with_plm207(
    classic_spark: Any, tmp_path: Path, case: str, kind: str
) -> None:
    ddl = "id BIGINT, v BIGINT"
    with _open(tmp_path, kind) as (s, catalog):
        name = _unique(kind)
        _write(s, classic_spark, name, [(0, 0)], ddl, WriteConfig("overwrite"), "seed")
        late = WriteConfig("overwrite", run_started_at=T1)
        _write(s, classic_spark, name, [(1, 10)], ddl, late, "r2:m")
        version = delta.DeltaMetrics(classic_spark).before_write(catalog.ident(_target(name)))
        with pytest.raises(WriteRefusedError):
            _write(s, classic_spark, name, [(1, 99), (2, 20)], ddl, _REFUSED[case], "r1:m")
        assert _rows(s, name, "id") == [(1, 10)]
        assert [c.name for c in catalog.table_schema(_target(name)) or ()] == ["id", "v"]
        commits = delta.history(classic_spark, catalog.ident(_target(name)), None)
        assert all(c.operation != "MERGE" for c in commits if c.version > (version or 0))


@pytest.mark.parametrize("mode", ["hard", "soft"])
def test_a_first_load_merge_leaves_out_the_deletes(
    classic_spark: Any, tmp_path: Path, mode: str
) -> None:
    deletes = DeleteSpec("op", "d", mode)  # type: ignore[arg-type]
    cfg = WriteConfig("merge", unique_key=("id",), deletes=deletes, run_started_at=T0)
    with _open(tmp_path) as (s, _):
        rows = [(1, "u"), (2, "d"), (3, None)]
        result = _write(s, classic_spark, "m", rows, "id BIGINT, op STRING", cfg, "r1:m")
        assert _rows(s, "m", "id") == [(1, "u"), (3, None)]
        assert result.rows_inserted == 2


@pytest.mark.parametrize("rows", [[], [(1, 10), (2, 20)]])
def test_replace_partitions_creates_a_missing_catalog_table(
    classic_spark: Any, tmp_path: Path, rows: list[tuple[Any, ...]]
) -> None:
    cfg = WriteConfig("replace_partitions", partition_by=("id",), run_started_at=T0)
    with _open(tmp_path, "spark_catalog") as (s, _):
        name = _unique("spark_catalog")
        result = _write(s, classic_spark, name, rows, "id BIGINT, v BIGINT", cfg, "r1:m")
        assert not result.skipped
        assert s.applied_token(_target(name), "r1:m") is not None
        assert _rows(s, name, "id") == rows


def test_a_stale_claim_does_not_let_a_retry_merge_over_a_later_run(
    classic_spark: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pipeloom_spark import write

    ddl = "id BIGINT, v BIGINT"
    merge = WriteConfig("merge", unique_key=("id",), run_started_at=T0)
    with _open(tmp_path) as (s, catalog):
        _write(s, classic_spark, "m", [(1, 10)], ddl, WriteConfig("overwrite"), "seed")
        real = write.merge_statement

        def boom(*args: Any) -> str:
            raise RuntimeError("the MERGE failed after the claim")

        monkeypatch.setattr(write, "merge_statement", boom)
        with pytest.raises(RuntimeError):
            _write(s, classic_spark, "m", [(1, 99)], ddl, merge, "r1:m")
        monkeypatch.setattr(write, "merge_statement", real)
        late = WriteConfig("overwrite", run_started_at=T1)
        _write(s, classic_spark, "m", [(5, 50)], ddl, late, "r2:m")
        with pytest.raises(WriteRefusedError):
            _write(s, classic_spark, "m", [(1, 99)], ddl, merge, "r1:m")
        assert _rows(s, "m", "id") == [(5, 50)]
        assert [c.name for c in catalog.table_schema(_target("m")) or ()] == ["id", "v"]


def test_a_crash_between_merge_and_token_commit_converges(
    classic_spark: Any, tmp_path: Path
) -> None:
    cfg = WriteConfig("merge", unique_key=("id",), run_started_at=T0)
    with _open(tmp_path) as (s, _):
        _write(
            s,
            classic_spark,
            "m",
            [(1, 10)],
            "id BIGINT, v BIGINT",
            WriteConfig("overwrite"),
            "seed",
        )
        writer = s._writer  # type: ignore[attr-defined]
        real = writer._save
        calls: list[int] = []

        def crash_once(w: Any, target: TargetRef) -> None:
            calls.append(1)
            if len(calls) == 2:  # the claim commit, the MERGE, then this token commit
                raise RuntimeError("killed before the token commit")
            real(w, target)

        writer._save = crash_once
        batch = [(1, 11), (2, 20)]
        with pytest.raises(RuntimeError):
            _write(s, classic_spark, "m", batch, "id BIGINT, v BIGINT", cfg, "r1:m")
        assert s.applied_token(_target("m"), "r1:m") is None
        again = _write(s, classic_spark, "m", batch, "id BIGINT, v BIGINT", cfg, "r1:m")
        assert not again.skipped
        assert _rows(s, "m", "id") == [(1, 11), (2, 20)]
        assert s.applied_token(_target("m"), "r1:m") is not None


def test_metrics_leave_out_a_foreign_commit_in_the_window(
    classic_spark: Any, tmp_path: Path
) -> None:
    with _open(tmp_path) as (s, catalog):
        assert isinstance(catalog, DeltaPathCatalog)
        _write(s, classic_spark, "m", [(1,)], "id BIGINT", WriteConfig("overwrite"), "seed")
        ident = catalog.ident(_target("m"))
        metrics = delta.DeltaMetrics(classic_spark)
        before = metrics.before_write(ident)
        foreign = classic_spark.createDataFrame([(5,), (6,), (7,)], "id BIGINT")
        foreign.write.format("delta").mode("append").save(catalog.path(_target("m")))
        _write(s, classic_spark, "m", [(2,)], "id BIGINT", WriteConfig("append"), "r1:m")
        got = metrics.after_write(ident, before, "r1:m", merge=False)
        assert got is not None and got.rows_written == 1


@pytest.mark.parametrize("strategy", ["append", "merge", "replace_partitions"])
def test_an_empty_batch_still_records_the_token(
    classic_spark: Any, tmp_path: Path, strategy: str
) -> None:
    cfg = WriteConfig(strategy, unique_key=("id",), partition_by=("id",))
    with _open(tmp_path) as (s, _):
        _write(s, classic_spark, "m", [(1,)], "id BIGINT", WriteConfig("overwrite"), "seed")
        result = _write(s, classic_spark, "m", [], "id BIGINT", cfg, "r1:m")
        assert not result.skipped
        assert s.applied_token(_target("m"), "r1:m") is not None
        assert _rows(s, "m", "id") == [(1,)]


@pytest.mark.parametrize(
    ("ddl", "old", "new"),
    [
        ("p STRING", ["a'b", None, "x"], "a'b"),
        ("p STRING", ["\\", "é", "x"], "é"),
        ("p DATE", [date(2026, 1, 1), None, date(2026, 1, 2)], date(2026, 1, 1)),
        ("p TIMESTAMP", [T0, None, T1], T1),
        ("p DECIMAL(10,2)", [Decimal("1.50"), None, Decimal("2.50")], Decimal("2.50")),
        ("p STRING", ["a", None, "b"], None),
    ],
)
def test_replace_where_handles_each_partition_value_type(
    classic_spark: Any, tmp_path: Path, ddl: str, old: list[Any], new: Any
) -> None:
    schema = f"{ddl}, v BIGINT"
    cfg = WriteConfig("replace_partitions", partition_by=("p",))
    with _open(tmp_path) as (s, _):
        _write(
            s, classic_spark, "m", [(p, 1) for p in old], schema, WriteConfig("overwrite"), "seed"
        )
        _write(s, classic_spark, "m", [(new, 9)], schema, cfg, "r1:m")
        got = _rows(s, "m", "v")
        want = [*[(p, 1) for p in old if p != new], (new, 9)]
        assert sorted(got, key=lambda r: (r[1], str(r[0]))) == sorted(
            want, key=lambda r: (r[1], str(r[0]))
        )


def test_a_removed_column_runs_a_migration_commit_first(classic_spark: Any, tmp_path: Path) -> None:
    columns = (Column("id", "int64"),)
    cfg = WriteConfig("append", columns=columns)
    with _open(tmp_path) as (s, catalog):
        _write(
            s,
            classic_spark,
            "m",
            [(1, "x")],
            "id BIGINT, extra STRING",
            WriteConfig("overwrite"),
            "seed",
        )
        _write(s, classic_spark, "m", [(2,)], "id BIGINT", cfg, "r1:m")
        assert [c.name for c in catalog.table_schema(_target("m")) or ()] == ["id"]
        assert _rows(s, "m", "id") == [(1,), (2,)]
        commits = delta.history(classic_spark, catalog.ident(_target("m")), None)
        assert any('"migration_for":"r1:m"' in (c.user_metadata or "") for c in commits)
        assert s.applied_token(_target("m"), "r1:m") is not None
