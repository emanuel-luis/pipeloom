"""Orders from an untrusted partner feed: expectations judge each batch, the contract pins the
schema, data tests judge the table after the write."""

from pipeloom import model


# --8<-- [start:orders]
@model(
    columns=[
        {"name": "order_id", "dtype": "int64", "nullable": False},
        {"name": "customer_id", "dtype": "int64", "nullable": False},
        {"name": "amount", "dtype": "float64", "nullable": False},
        {"name": "status", "dtype": "string"},
        {"name": "channel", "dtype": "string"},
    ],
)
def orders(ctx, orders):
    # select * on purpose: a column the partner adds reaches the contract and breaks it.
    return ctx.sql("select * from orders")


# --8<-- [end:orders]
