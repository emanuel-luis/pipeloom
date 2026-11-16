"""A Python-first mart next to the YAML models."""

from typing import Any

from pipeloom import ModelContext, model


@model(tags=["mart"])
def top_customers(ctx: ModelContext, rpt_revenue: Any) -> Any:
    return ctx.sql("select customer_id, revenue from rpt_revenue where revenue > 0")
