"""Revenue in US dollars: convert each order, net out its refunds, total each day.

The three models use `frame_api="narwhals"`, so the same function runs on DuckDB and polars. The
first parameter is `ctx`, which none of them needs, so a plain pytest can pass `None`."""

import narwhals.stable.v2 as nw

from pipeloom import Expectation, model, not_null, ref, source


# --8<-- [start:orders_usd]
@model(
    frame_api="narwhals",
    inputs={"orders": source("shop", "orders"), "fx_rates": source("shop", "fx_rates")},
    expectations=[
        Expectation(name="rate_found", check=not_null("amount_usd"), on_violation="fail")
    ],
)
def orders_usd(ctx, orders, fx_rates):
    """Drop cancelled orders and convert the rest with the rate of the order date."""
    return (
        orders.filter(nw.col("status") != "cancelled")
        .join(
            fx_rates,
            left_on=["order_date", "currency"],
            right_on=["rate_date", "currency"],
            how="left",
        )
        .with_columns((nw.col("amount") * nw.col("rate")).round(2).alias("amount_usd"))
        .select("order_id", "order_date", "customer_id", "rate", "amount_usd")
    )


# --8<-- [end:orders_usd]


# --8<-- [start:net_revenue]
@model(
    frame_api="narwhals",
    inputs={"orders_usd": ref("orders_usd"), "refunds": source("shop", "refunds")},
)
def net_revenue(ctx, orders_usd, refunds):
    """Subtract the refunds of each order, converted at the rate of the order."""
    refunded = refunds.group_by("order_id").agg(nw.col("refund_amount").sum().alias("refunded"))
    return (
        orders_usd.join(refunded, on="order_id", how="left")
        .with_columns(nw.col("refunded").fill_null(0.0))
        .with_columns(
            (nw.col("amount_usd") - (nw.col("refunded") * nw.col("rate")).round(2)).alias("net_usd")
        )
        .select("order_id", "order_date", "customer_id", "amount_usd", "net_usd")
    )


# --8<-- [end:net_revenue]


# --8<-- [start:daily_revenue]
@model(frame_api="narwhals", inputs={"net_revenue": ref("net_revenue")})
def daily_revenue(ctx, net_revenue):
    """Orders and net revenue per day, with a running total."""
    daily = net_revenue.group_by("order_date").agg(
        nw.col("order_id").count().alias("orders"),
        nw.col("net_usd").sum().round(2).alias("net_usd"),
    )
    return daily.with_columns(
        nw.col("net_usd").cum_sum().over(order_by="order_date").round(2).alias("running_net_usd")
    )


# --8<-- [end:daily_revenue]
