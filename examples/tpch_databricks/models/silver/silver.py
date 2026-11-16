"""Silver: one row per order with its customer, nation and region; one row per line item with
its net revenue. Each parameter binds the bronze model of the same name."""

from pipeloom import Expectation, ModelContext, model, not_null, unique


@model(
    expectations=[
        Expectation(
            name="keys_present", check=not_null("order_key", "customer_key"), on_violation="fail"
        ),
        Expectation(name="order_key_unique", check=unique("order_key"), on_violation="fail"),
    ],
)
def orders_enriched(ctx: ModelContext, orders, customer, nation, region):
    return ctx.sql("""
        select
          o.o_orderkey as order_key,
          o.o_custkey as customer_key,
          c.c_name as customer_name,
          c.c_mktsegment as market_segment,
          n.n_name as nation,
          r.r_name as region,
          o.o_orderdate as order_date,
          o.o_orderstatus as order_status,
          o.o_totalprice as total_price
        from orders o
        join customer c on o.o_custkey = c.c_custkey
        join nation n on c.c_nationkey = n.n_nationkey
        join region r on n.n_regionkey = r.r_regionkey
    """)


@model(
    expectations=[
        Expectation(
            name="keys_present", check=not_null("order_key", "line_number"), on_violation="fail"
        ),
    ],
)
def lineitem_revenue(ctx: ModelContext, lineitem):
    return ctx.sql("""
        select
          l_orderkey as order_key,
          l_linenumber as line_number,
          l_quantity as quantity,
          l_extendedprice as extended_price,
          l_discount as discount,
          l_extendedprice * (1 - l_discount) as net_revenue,
          l_shipdate as ship_date
        from lineitem
    """)
