"""The customers the feed may refer to."""

from pipeloom import model


@model
def customers(ctx, customers):
    return ctx.sql("select cast(id as bigint) as id, name from customers")
