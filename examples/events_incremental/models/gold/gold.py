"""Gold: one row per day, rebuilt from silver."""

from pipeloom import ModelContext, model


@model
def daily_events(ctx: ModelContext, events_clean):
    return ctx.sql("""
        select
          cast(event_ts as date) as event_date,
          count(*) as events,
          count(distinct user_id) as users,
          sum(amount) as revenue
        from events_clean
        group by 1
    """)
