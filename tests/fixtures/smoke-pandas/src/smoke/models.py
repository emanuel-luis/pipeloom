"""Python models on pandas DataFrames, and Narwhals frames for fct_orders. No SQL, no views."""

from typing import Any

import narwhals.stable.v2 as nw

from pipeloom import model, ref, source


@model(
    inputs={"raw_customers": source("raw", "raw_customers")},
)
def stg_customers(ctx: Any, raw_customers: Any) -> Any:
    out = raw_customers[["id", "email"]].copy()
    out["email"] = out["email"].str.lower()
    return out


@model(
    inputs={"raw_orders": source("raw", "raw_orders")},
)
def stg_orders(ctx: Any, raw_orders: Any) -> Any:
    return raw_orders[["id", "customer_id", "amount"]]


@model(
    inputs={"orders": ref("stg_orders"), "customers": ref("stg_customers")},
    materialized="ephemeral",
)
def orders_enriched(ctx: Any, orders: Any, customers: Any) -> Any:
    joined = orders.merge(customers, left_on="customer_id", right_on="id", suffixes=("", "_c"))
    return joined[["id", "customer_id", "email", "amount"]]


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
    return enriched[["customer_id", "email"]].drop_duplicates().reset_index(drop=True)


@model
def rpt_revenue(ctx: Any, fct_orders: Any) -> Any:
    out = fct_orders.groupby("customer_id", as_index=False)["amount"].sum()
    return out.rename(columns={"amount": "revenue"})
