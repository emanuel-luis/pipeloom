"""One portable model and one set of input rows, shared by every engine of the matrix."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any

import narwhals.stable.v2 as nw

from pipeloom.engine.protocols import Catalog, Engine, EngineSession
from pipeloom.engine.types import Column
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.reference import sort_key

Rows = Sequence[Mapping[str, Any]]

ORDERS_SCHEMA = (
    Column("order_id", "int64"),
    Column("customer_id", "int64"),
    Column("amount", "float64"),
    Column("status", "string"),
)
ORDERS: Rows = (
    {"order_id": 1, "customer_id": 10, "amount": 10.5, "status": "paid"},
    {"order_id": 2, "customer_id": 10, "amount": 4.25, "status": "paid"},
    {"order_id": 3, "customer_id": 20, "amount": 8.0, "status": "paid"},
    {"order_id": 4, "customer_id": 20, "amount": 99.0, "status": "refunded"},
    {"order_id": 5, "customer_id": 30, "amount": 2.5, "status": "paid"},
    {"order_id": 6, "customer_id": 40, "amount": 7.0, "status": "paid"},
)
CUSTOMERS_SCHEMA = (
    Column("customer_id", "int64"),
    Column("name", "string"),
    Column("region", "string"),
)
CUSTOMERS: Rows = (
    {"customer_id": 10, "name": "Ana", "region": "north"},
    {"customer_id": 20, "name": "Bia", "region": "south"},
    {"customer_id": 30, "name": "Caio", "region": "north"},
)
"""Customer 40 has no row, so the inner join drops order 6."""

EXPECTED: list[dict[str, Any]] = [
    {"region": "north", "orders": 3, "total": 34.5},
    {"region": "south", "orders": 1, "total": 16.0},
]


def portable_model(orders: Any, customers: Any) -> Any:
    """A filter, a join, a with_columns expression and a group_by aggregate; no row order is
    assumed, so it runs on lazy and eager frames alike."""
    paid = orders.filter(nw.col("status") == "paid")
    joined = paid.join(customers, on="customer_id", how="inner")
    doubled = joined.with_columns((nw.col("amount") * 2).alias("amount_x2"))
    return doubled.group_by("region").agg(
        nw.col("order_id").count().alias("orders"), nw.col("amount_x2").sum().alias("total")
    )


def run_model(session: EngineSession) -> list[dict[str, Any]]:
    """Feed the inputs through the engine as the runner does, and read the rows back."""
    frames = [
        nw.from_native(session.from_rows(rows, schema).native)
        for rows, schema in ((ORDERS, ORDERS_SCHEMA), (CUSTOMERS, CUSTOMERS_SCHEMA))
    ]
    out = nw.to_native(portable_model(*frames), pass_through=True)
    return sorted(session.to_rows(session.relation(out)), key=sort_key)


def open_session(
    engine: Engine, catalog: Catalog, tmp_path: Path
) -> AbstractContextManager[EngineSession]:
    ctx = RuntimeContext(
        run_id="matrix",
        invocation_id="matrix",
        project="matrix",
        target="test",
        project_dir=tmp_path,
        clock=FakeClock(),
        identity=Identity("tester", "host"),
        vars={},
    )
    return engine.session(ctx, catalog=catalog)


SessionOpener = Callable[[Path], AbstractContextManager[EngineSession]]
__all__ = ["EXPECTED", "SessionOpener", "open_session", "portable_model", "run_model"]
