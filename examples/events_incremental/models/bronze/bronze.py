"""Bronze: every event of every daily file, typed. Rebuilt on each run."""

from pipeloom import ModelContext, model


@model
def events(ctx: ModelContext, events):
    # The parameter binds the source table `events`: a model never reads itself.
    return ctx.sql("""
        select
          event_id,
          user_id,
          event_type,
          cast(event_ts as timestamp) as event_ts,
          cast(amount as double) as amount
        from events
    """)
