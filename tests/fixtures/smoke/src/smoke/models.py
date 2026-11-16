"""Python models: native polars frames, and Narwhals frames for fct_orders."""

from typing import Any

import narwhals.stable.v2 as nw

from pipeloom import model, ref


@model(
    inputs={"orders": ref("stg_orders"), "customers": ref("stg_customers")},
    materialized="ephemeral",
)
def orders_enriched(ctx: Any, orders: Any, customers: Any) -> Any:
    joined = orders.join(customers, left_on="customer_id", right_on="id")
    return joined.select("id", "customer_id", "email", "amount")


@model(
    inputs={"enriched": ref("orders_enriched")},
    frame_api="narwhals",
    write={"strategy": "append"},
)
def fct_orders(ctx: Any, enriched: Any) -> Any:
    return enriched.filter(nw.col("amount") > 1)


@model(
    inputs={"enriched": ref("orders_enriched")},
)
def dim_customers(ctx: Any, enriched: Any) -> Any:
    return enriched.select("customer_id", "email").unique()
