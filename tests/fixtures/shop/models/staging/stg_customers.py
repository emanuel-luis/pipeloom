from typing import Any

from pipeloom import model


@model
def stg_customers(ctx: Any, raw_customers: Any) -> Any:
    return ctx.sql("select id, lower(email) as email from raw_customers")
