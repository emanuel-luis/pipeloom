from typing import Any

from pipeloom import model


@model
def stg_payments(ctx: Any, raw_payments: Any) -> Any:
    return ctx.sql("select order_id, amount from raw_payments")
