"""Bronze: untouched copies of the TPC-H tables, with the original column names."""

from typing import Any


def orders(ctx: Any, tpch_orders: Any) -> Any:
    return tpch_orders


def lineitem(ctx: Any, tpch_lineitem: Any) -> Any:
    return tpch_lineitem


def customer(ctx: Any, tpch_customer: Any) -> Any:
    return tpch_customer


def nation(ctx: Any, tpch_nation: Any) -> Any:
    return tpch_nation


def region(ctx: Any, tpch_region: Any) -> Any:
    return tpch_region
