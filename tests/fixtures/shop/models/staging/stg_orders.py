from typing import Any

from pipeloom import model


@model
def stg_orders(ctx: Any, raw_orders: Any) -> Any:
    return ctx.sql("select id, customer_id, amount from raw_orders")
