from pipeloom import model, source


@model(
    inputs={"raw_orders": source("raw", "raw_orders")},
    tags=["staging"],
)
def stg_orders(ctx, raw_orders):
    return ctx.sql("select * from raw_orders where country = '${var:country}'")
