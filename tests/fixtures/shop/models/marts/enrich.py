"""An ephemeral model: orders with their customer."""

from typing import Any

from pipeloom import model, ref


@model(
    inputs={"orders": ref("stg_orders"), "customers": ref("stg_customers")},
    materialized="ephemeral",
)
def orders_enriched(ctx: Any, orders: Any, customers: Any) -> Any:
    return orders
