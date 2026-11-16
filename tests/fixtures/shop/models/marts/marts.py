from pipeloom import model


@model
def rpt_revenue(ctx, fct_orders):
    return ctx.sql(
        "select customer_id, sum(amount) as revenue from fct_orders group by customer_id"
    )
