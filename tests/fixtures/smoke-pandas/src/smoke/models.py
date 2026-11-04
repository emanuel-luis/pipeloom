"""Python models on pandas DataFrames, and Narwhals frames for fct_orders. No SQL, no views."""

from typing import Any

import narwhals.stable.v2 as nw


def stg_customers(ctx: Any, raw_customers: Any) -> Any:
    out = raw_customers[["id", "email"]].copy()
    out["email"] = out["email"].str.lower()
    return out


def stg_orders(ctx: Any, raw_orders: Any) -> Any:
    return raw_orders[["id", "customer_id", "amount"]]


def orders_enriched(ctx: Any, orders: Any, customers: Any) -> Any:
    joined = orders.merge(customers, left_on="customer_id", right_on="id", suffixes=("", "_c"))
    return joined[["id", "customer_id", "email", "amount"]]


def fct_orders(ctx: Any, enriched: Any) -> Any:
    return enriched.filter(nw.col("amount") > 1)


def dim_customers(ctx: Any, enriched: Any) -> Any:
    return enriched[["customer_id", "email"]].drop_duplicates().reset_index(drop=True)


def rpt_revenue(ctx: Any, fct_orders: Any) -> Any:
    out = fct_orders.groupby("customer_id", as_index=False)["amount"].sum()
    return out.rename(columns={"amount": "revenue"})
