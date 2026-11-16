"""Bronze: untouched copies of the TPC-H tables, with the original column names.

A model never reads itself, so each parameter binds the source table of the same name.
"""

from pipeloom import ModelContext, model


@model
def orders(ctx: ModelContext, orders):
    return ctx.sql("select * from orders")


@model
def lineitem(ctx: ModelContext, lineitem):
    return ctx.sql("select * from lineitem")


@model
def customer(ctx: ModelContext, customer):
    return ctx.sql("select * from customer")


@model
def nation(ctx: ModelContext, nation):
    return ctx.sql("select * from nation")


@model
def region(ctx: ModelContext, region):
    return ctx.sql("select * from region")
