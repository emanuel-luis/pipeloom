from pipeloom import model


@model(
    tags=["stable"],
)
def a(ctx):
    return ctx.sql("select 1 as id, 'a' as label")


@model(
    tags=["stable"],
)
def b(ctx, a):
    return ctx.sql("select id, label, id + 1 as next_id from a")
