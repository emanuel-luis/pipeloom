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


@model(
    inputs={"raw_payments": source("raw", "raw_payments")},
)
def stg_payments(ctx, raw_payments):
    return ctx.sql("select order_id, amount from raw_payments")
