"""Spark tests opt in with PIPELOOM_TEST_SPARK=classic or connect.

They also need pyspark, a `java` on PATH and a non-Windows host; otherwise they skip. Classic and
Connect cannot share a Python process, so they are two pytest invocations.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import math
import os
import re
import shutil
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeloom.engine.protocols import Catalog, EngineSession
from pipeloom.engine.types import Relation, Schema
from pipeloom.testing.contract.engine import EngineHarness
from pipeloom_spark.engine import SparkEngine
from pipeloom_spark.session import open_session
from pipeloom_spark.spec import SparkEngineOptions
from pipeloom_spark.types import to_spark

CONF = {
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "2",
    "spark.default.parallelism": "2",
    "spark.ui.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}


def _skip_reason(kind: str) -> str | None:
    if sys.platform == "win32":
        return "Spark runs in WSL or Linux only"
    if os.environ.get("PIPELOOM_TEST_SPARK") != kind:
        return f"set PIPELOOM_TEST_SPARK={kind} to run these tests"
    if importlib.util.find_spec("pyspark") is None:
        return "pyspark is not installed (extra pipeloom-spark[local])"
    if shutil.which("java") is None:
        return "java is not on PATH"
    return None


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        for marker, kind in (("spark", "classic"), ("spark_connect", "connect")):
            if item.get_closest_marker(marker) is not None:
                reason = _skip_reason(kind)
                if reason is not None:
                    item.add_marker(pytest.mark.skip(reason=reason))


def _session(
    mode: str, remote: str | None, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[Any]:
    warehouse = tmp_path_factory.mktemp("warehouse").as_posix()
    opts = SparkEngineOptions(
        mode=mode,  # type: ignore[arg-type]
        master="local[2]",
        remote=remote,
        conf={**CONF, "spark.sql.warehouse.dir": warehouse},
    )
    with open_session(opts, {}) as handle:
        yield handle.spark


@pytest.fixture(scope="module")
def classic_spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    yield from _session("local", None, tmp_path_factory)


@pytest.fixture(scope="module")
def connect_spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    yield from _session("connect", "local[2]", tmp_path_factory)


# -- the engine contract harness, shared by the classic and Connect modules ----------------------

Rows = Sequence[Mapping[str, Any]]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _arrow_type(logical: str) -> Any:
    import pyarrow as pa

    simple = {
        "int64": pa.int64(),
        "float64": pa.float64(),
        "string": pa.string(),
        "boolean": pa.bool_(),
        "date": pa.date32(),
        "timestamp[us, UTC]": pa.timestamp("us", tz="UTC"),
    }
    if logical in simple:
        return simple[logical]
    m = re.fullmatch(r"decimal\((\d+),(\d+)\)", logical)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))
    raise ValueError(logical)


def _frame(spark: Any, rows: Rows, schema: Schema) -> Any:
    from pyspark.sql.types import StructField, StructType

    st = StructType([StructField(c.name, to_spark(c.dtype), True) for c in schema])
    return spark.createDataFrame([tuple(r[c.name] for c in schema) for r in rows], st)


def _write_source(spark: Any, fmt: str, path: Path, rows: Rows, schema: Schema) -> None:
    names = [c.name for c in schema]
    if fmt == "parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.table(
            {c.name: pa.array([r[c.name] for r in rows], _arrow_type(c.dtype)) for c in schema}
        )
        pq.write_table(table, path)
    elif fmt == "csv":
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(names)
            writer.writerows([["" if r[n] is None else r[n] for n in names] for r in rows])
    elif fmt == "json":
        path.write_text(json.dumps([dict(r) for r in rows]), encoding="utf-8")
    elif fmt == "ndjson":
        path.write_text("\n".join(json.dumps(dict(r)) for r in rows) + "\n", encoding="utf-8")
    elif fmt == "delta":
        _frame(spark, rows, schema).write.format("delta").save(path.as_posix())
    else:
        raise ValueError(fmt)


def _to_rows(native: Any) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    stamps = [f.name for f in native.schema.fields if type(f.dataType).__name__ == "TimestampType"]
    cols = [
        F.expr(f"unix_micros(`{f.name}`)").alias(f.name)
        if f.name in stamps
        else F.col(f"`{f.name}`")
        for f in native.schema.fields
    ]
    out = []
    for row in native.select(*cols).collect():
        d = row.asDict()
        for name in stamps:
            if d[name] is not None:
                d[name] = _EPOCH + timedelta(microseconds=d[name])
        out.append(d)
    return out


def _settings(spark: Any) -> dict[str, Any]:
    snap: dict[str, Any] = {r[0]: r[1] for r in spark.sql("SET").collect()}
    if hasattr(spark, "sparkContext"):  # classic only
        snap.update({f"ctx.{k}": v for k, v in spark.sparkContext.getConf().getAll()})
    return snap


def _slow(spark: Any) -> Callable[[EngineSession, float], Relation]:
    def slow(session: EngineSession, seconds: float) -> Relation:
        spark.range(0, 10**13).selectExpr("sum(sqrt(id))").collect()  # runs until interrupted
        return session.relation(spark.range(1))

    return slow


def _num(value: Any) -> Any:
    return None if value is None or math.isnan(value) else value


EXPR_CASES = (
    ("f > 0", lambda r: None if _num(r["f"]) is None else r["f"] > 0),
    ("id < 3", lambda r: None if r["id"] is None else r["id"] < 3),
)
"""Spark SQL `expr` checks and the predicates they stand for; NaN reads as null."""


@pytest.fixture
def harness_factory() -> Callable[[Any, Catalog, Path], EngineHarness]:
    def build(spark: Any, catalog: Catalog, tmp_path: Path) -> EngineHarness:
        engine = SparkEngine(SparkEngineOptions(mode="active"))
        if not catalog.capabilities.views:  # type: ignore[attr-defined]
            # a catalog without views leaves the engine's view capability unused
            engine.capabilities = replace(engine.capabilities, views=False)
        return EngineHarness(
            engine=engine,
            catalog=catalog,
            make_native=lambda rows, schema: _frame(spark, rows, schema),
            to_rows=_to_rows,
            write_source=lambda fmt, path, rows, schema: _write_source(
                spark, fmt, path, rows, schema
            ),
            failing_relation=lambda s: s.relation(spark.range(1).selectExpr("raise_error('boom')")),
            settings_snapshot=lambda: _settings(spark),
            slow_relation=_slow(spark),
            schema_on_read=True,
            update_condition="source.v > target.v",
            expr_cases=EXPR_CASES,
        )

    return build
