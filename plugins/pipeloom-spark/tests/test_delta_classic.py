"""Delta write behavior beyond the contract, on classic local Spark with the delta_path catalog."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.errors import WriteRefusedError
from pipeloom.engine.protocols import EngineSession
from pipeloom.engine.types import Column, ReadSpec, TargetRef, WriteConfig
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.clock import FakeClock
from pipeloom_spark import delta
from pipeloom_spark.catalog import DeltaPathCatalog
from pipeloom_spark.engine import SparkEngine
from pipeloom_spark.spec import SparkEngineOptions

pytestmark = [pytest.mark.spark, pytest.mark.timeout(300)]

T0 = datetime(2026, 1, 1, tzinfo=UTC)
T1 = datetime(2026, 1, 2, tzinfo=UTC)


def _target(name: str) -> TargetRef:
    return TargetRef(None, "main", name)


@contextmanager
def _open(tmp_path: Path) -> Iterator[tuple[EngineSession, DeltaPathCatalog]]:
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
    catalog = DeltaPathCatalog(str(tmp_path / "wh"))
    with SparkEngine(SparkEngineOptions(mode="active")).session(ctx, catalog=catalog) as s:
        yield s, catalog


def _write(
    s: EngineSession,
    spark: Any,
    name: str,
    rows: list[tuple[Any, ...]],
    ddl: str,
    cfg: WriteConfig,
    token: str,
) -> Any:
    frame = s.relation(spark.createDataFrame(rows, ddl))
    return s.materializer(cfg.strategy).materialize(frame, _target(name), cfg, token)


def _rows(s: EngineSession, name: str, order: str) -> list[tuple[Any, ...]]:
    frame = s.reader("table").read(ReadSpec("table", table=_target(name))).native
    return [
        tuple(v.astimezone(UTC) if isinstance(v, datetime) else v for v in r)
        for r in frame.orderBy(order).collect()
    ]


def test_a_later_transaction_version_makes_an_earlier_run_fail_with_plm207(
    classic_spark: Any, tmp_path: Path
) -> None:
    with _open(tmp_path) as (s, _):
        late = WriteConfig("append", run_started_at=T1)
        _write(s, classic_spark, "m", [(1,)], "id BIGINT", late, "r2:m")
        early = WriteConfig("append", run_started_at=T0)
        with pytest.raises(WriteRefusedError):
            _write(s, classic_spark, "m", [(2,)], "id BIGINT", early, "r1:m")
        assert _rows(s, "m", "id") == [(1,)]


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
            if len(calls) == 1:
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
