"""Plain pytest on the model functions: no pipeloom run, no engine session.

A decorated function is still a function. These models use narwhals frames, so the test wraps a
polars frame and passes `None` for `ctx`, which none of them reads."""

import importlib.util
from pathlib import Path

import narwhals.stable.v2 as nw
import polars as pl

_path = Path(__file__).resolve().parents[1] / "models" / "revenue.py"
_spec = importlib.util.spec_from_file_location("revenue", _path)
assert _spec and _spec.loader
revenue = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(revenue)


def frame(rows):
    return nw.from_native(pl.LazyFrame(rows))


def rows(result):
    return sorted(result.collect().rows(named=True), key=lambda r: r["order_id"])


# --8<-- [start:direct]
def test_orders_usd_drops_cancelled_orders():
    orders = frame(
        {
            "order_id": [1, 2],
            "order_date": ["2026-03-01", "2026-03-01"],
            "customer_id": ["c1", "c2"],
            "currency": ["USD", "USD"],
            "amount": [10.0, 99.0],
            "status": ["paid", "cancelled"],
        }
    )
    fx_rates = frame({"rate_date": ["2026-03-01"], "currency": ["USD"], "rate": [1.0]})
    out = rows(revenue.orders_usd(None, orders, fx_rates))
    assert [(r["order_id"], r["amount_usd"]) for r in out] == [(1, 10.0)]


# --8<-- [end:direct]


def test_net_revenue_keeps_orders_without_refunds():
    orders_usd = frame(
        {
            "order_id": [1, 2],
            "order_date": ["2026-03-01", "2026-03-01"],
            "customer_id": ["c1", "c2"],
            "rate": [2.0, 1.0],
            "amount_usd": [100.0, 40.0],
        }
    )
    refunds = frame({"refund_id": ["r1", "r2"], "order_id": [1, 1], "refund_amount": [5.0, 5.0]})
    out = rows(revenue.net_revenue(None, orders_usd, refunds))
    assert [(r["order_id"], r["net_usd"]) for r in out] == [(1, 80.0), (2, 40.0)]
