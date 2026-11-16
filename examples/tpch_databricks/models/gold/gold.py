"""Gold: two reports built from the silver tables."""

from pipeloom import Expectation, ModelContext, model, row_count


@model(expectations=[Expectation(name="not_empty", check=row_count(min=1), on_violation="fail")])
def revenue_by_region_month(ctx: ModelContext, orders_enriched, lineitem_revenue):
    return ctx.sql("""
        select
          o.region as region,
          o.nation as nation,
          cast(extract(year from o.order_date) as int) as order_year,
          cast(extract(month from o.order_date) as int) as order_month,
          count(distinct o.order_key) as orders,
          round(sum(l.net_revenue), 2) as net_revenue
        from orders_enriched o
        join lineitem_revenue l on o.order_key = l.order_key
        group by
          o.region,
          o.nation,
          extract(year from o.order_date),
          extract(month from o.order_date)
    """)


@model(expectations=[Expectation(name="not_empty", check=row_count(min=1), on_violation="fail")])
def top_customers(ctx: ModelContext, orders_enriched, lineitem_revenue):
    return ctx.sql("""
        select
          o.customer_key as customer_key,
          o.customer_name as customer_name,
          o.nation as nation,
          o.region as region,
          count(distinct o.order_key) as orders,
          round(sum(l.net_revenue), 2) as net_revenue
        from orders_enriched o
        join lineitem_revenue l on o.order_key = l.order_key
        group by o.customer_key, o.customer_name, o.nation, o.region
        order by net_revenue desc, customer_key
        limit 10
    """)
