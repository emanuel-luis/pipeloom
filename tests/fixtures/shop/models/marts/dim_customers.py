from typing import Any

from pipeloom import model, ref


@model(inputs={"enriched": ref("orders_enriched")}, tags=["mart"])
def dim_customers(ctx: Any, enriched: Any) -> Any:
    return enriched
