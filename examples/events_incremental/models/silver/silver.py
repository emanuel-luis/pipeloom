"""Silver: one row per event, loaded incrementally and merged on event_id."""

from pipeloom import Expectation, ModelContext, model, not_null, unique


@model(
    materialized="incremental",
    incremental={"input": "events", "column": "event_ts", "lookback": "2h"},
    write={
        "strategy": "merge",
        "unique_key": ["event_id"],
        # A resent event is identical, so a matched row is only touched when a value changed.
        "update_condition": (
            "(source.user_id is distinct from target.user_id)"
            " or (source.event_type is distinct from target.event_type)"
            " or (source.event_ts is distinct from target.event_ts)"
            " or (source.amount is distinct from target.amount)"
        ),
    },
    expectations=[
        Expectation(
            name="keys_present", check=not_null("event_id", "event_ts"), on_violation="fail"
        ),
        Expectation(name="event_id_unique", check=unique("event_id"), on_violation="fail"),
    ],
)
def events_clean(ctx: ModelContext, events):
    # The batch holds the window of late rows plus the new rows. A copy of an event that is
    # already in the window would repeat a merge key (PLM206), so collapse exact copies first.
    return ctx.sql("""
        select distinct event_id, user_id, event_type, event_ts, amount
        from events
    """)
