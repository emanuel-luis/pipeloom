"""Silver: orders with their customer, nation and region; line items with their net revenue.

Each parameter binds the bronze model of the same name.
"""

import pandas as pd

from pipeloom import Expectation, model, not_null, unique


@model(
    expectations=[
        Expectation(
            name="keys_present", check=not_null("order_key", "customer_key"), on_violation="fail"
        ),
        Expectation(name="order_key_unique", check=unique("order_key"), on_violation="fail"),
    ],
)
def orders_enriched(
    orders: pd.DataFrame, customer: pd.DataFrame, nation: pd.DataFrame, region: pd.DataFrame
) -> pd.DataFrame:
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


@model(
    expectations=[
        Expectation(
            name="keys_present", check=not_null("order_key", "line_number"), on_violation="fail"
        ),
    ],
)
def lineitem_revenue(lineitem: pd.DataFrame) -> pd.DataFrame:
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
