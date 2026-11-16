from typing import Any

from pipeloom import model


@model
def rpt_revenue(ctx: Any, fct_orders: Any) -> Any:
    return ctx.sql(
        "select customer_id, sum(amount) as revenue from fct_orders group by customer_id"
    )
