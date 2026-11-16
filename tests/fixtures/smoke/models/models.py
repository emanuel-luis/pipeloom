from pipeloom import model, source


@model(
    inputs={"raw_customers": source("raw", "raw_customers")},
)
def stg_customers(ctx, raw_customers):
    return ctx.sql("select id, lower(email) as email from raw_customers")


@model(
    inputs={"raw_orders": source("raw", "raw_orders")},
)
def stg_orders(ctx, raw_orders):
    return ctx.sql("select id, customer_id, amount from raw_orders")


@model
def rpt_revenue(ctx, fct_orders):
    return ctx.sql(
        "select customer_id, sum(amount) as revenue from fct_orders group by customer_id"
    )


@model(
    materialized="view",
)
def v_big_customers(ctx, rpt_revenue):
    return ctx.sql("select customer_id, revenue from rpt_revenue where revenue >= 7.5")
