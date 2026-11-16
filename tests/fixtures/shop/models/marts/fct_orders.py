from typing import Any

from pipeloom import model, ref


@model(
    inputs={"enriched": ref("orders_enriched"), "payments": ref("stg_payments")},
    tags=["mart"],
    write={"strategy": "append"},
)
def fct_orders(ctx: Any, enriched: Any, payments: Any) -> Any:
    return enriched
