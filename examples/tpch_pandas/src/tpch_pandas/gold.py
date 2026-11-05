"""Gold: revenue by region and month, and the ten customers with the most net revenue."""

from typing import Any

import pandas as pd


def _joined(orders_enriched: Any, lineitem_revenue: Any) -> Any:
    return orders_enriched.merge(lineitem_revenue[["order_key", "net_revenue"]], on="order_key")


def revenue_by_region_month(ctx: Any, orders_enriched: Any, lineitem_revenue: Any) -> Any:
    df = _joined(orders_enriched, lineitem_revenue)
    dates = pd.to_datetime(df["order_date"])
    df["order_year"] = dates.dt.year.astype("int32")
    df["order_month"] = dates.dt.month.astype("int32")
    out = df.groupby(["region", "nation", "order_year", "order_month"], as_index=False).agg(
        orders=("order_key", "nunique"), net_revenue=("net_revenue", "sum")
    )
    out["net_revenue"] = out["net_revenue"].round(2)
    return out


def top_customers(ctx: Any, orders_enriched: Any, lineitem_revenue: Any) -> Any:
    df = _joined(orders_enriched, lineitem_revenue)
    out = df.groupby(["customer_key", "customer_name", "nation", "region"], as_index=False).agg(
        orders=("order_key", "nunique"), net_revenue=("net_revenue", "sum")
    )
    out["net_revenue"] = out["net_revenue"].round(2)
    out = out.sort_values(["net_revenue", "customer_key"], ascending=[False, True])
    return out.head(10).reset_index(drop=True)
