"""Silver: orders with their customer, nation and region; line items with their net revenue."""

from typing import Any


def orders_enriched(ctx: Any, orders: Any, customer: Any, nation: Any, region: Any) -> Any:
    joined = (
        orders.merge(customer, left_on="o_custkey", right_on="c_custkey")
        .merge(nation, left_on="c_nationkey", right_on="n_nationkey")
        .merge(region, left_on="n_regionkey", right_on="r_regionkey")
    )
    return joined.rename(
        columns={
            "o_orderkey": "order_key",
            "o_custkey": "customer_key",
            "c_name": "customer_name",
            "c_mktsegment": "market_segment",
            "n_name": "nation",
            "r_name": "region",
            "o_orderdate": "order_date",
            "o_orderstatus": "order_status",
            "o_totalprice": "total_price",
        }
    )[
        [
            "order_key",
            "customer_key",
            "customer_name",
            "market_segment",
            "nation",
            "region",
            "order_date",
            "order_status",
            "total_price",
        ]
    ]


def lineitem_revenue(ctx: Any, lineitem: Any) -> Any:
    out = lineitem.rename(
        columns={
            "l_orderkey": "order_key",
            "l_linenumber": "line_number",
            "l_quantity": "quantity",
            "l_extendedprice": "extended_price",
            "l_discount": "discount",
            "l_shipdate": "ship_date",
        }
    ).copy()
    out["net_revenue"] = out["extended_price"] * (1 - out["discount"])
    return out[
        [
            "order_key",
            "line_number",
            "quantity",
            "extended_price",
            "discount",
            "net_revenue",
            "ship_date",
        ]
    ]
