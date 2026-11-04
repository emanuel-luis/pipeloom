"""Python models: native polars frames, and Narwhals frames for fct_orders."""

from typing import Any

import narwhals.stable.v2 as nw


def orders_enriched(ctx: Any, orders: Any, customers: Any) -> Any:
    joined = orders.join(customers, left_on="customer_id", right_on="id")
    return joined.select("id", "customer_id", "email", "amount")


def fct_orders(ctx: Any, enriched: Any) -> Any:
    return enriched.filter(nw.col("amount") > 1)


def dim_customers(ctx: Any, enriched: Any) -> Any:
    return enriched.select("customer_id", "email").unique()
