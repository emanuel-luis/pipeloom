# Migrating within 1.x

Every 1.x release keeps 1.0 projects working. This page covers the changes you can opt into.

## From a watermark model to an interval model (1.3)

A 1.0 incremental model with `incremental.column` reads the rows after the largest value it has
seen. An interval model ([Interval models](incremental.md#interval-models)) runs windows of event
time and keeps a ledger of the windows it has processed. To convert a model without reprocessing
the history it already holds:

1. Find the model's watermark: the `watermark_after` of its row in the `run_results.json` of
   its last run. Say it is `2026-03-14T17:20:00Z`.
2. Change the config: drop `incremental.column` and add `incremental.interval`, with the same
   column as `event_time`, a `grain`, and `begin` set to the first day of your data. The write
   strategy must be `append`.

    ```python
    @model(
        inputs={"raw": source("raw", "events")},
        materialized="incremental",
        incremental={
            "input": "raw",
            "interval": {"event_time": "ts", "grain": "day", "begin": "2025-01-01"},
        },
    )
    def daily_events(ctx, raw):
        return raw
    ```

3. Seed the ledger with the time the table already holds, up to the start of the window that
   holds the watermark:

    ```bash
    pipeloom backfill --select daily_events --mark-done --start 2025-01-01 --end 2026-03-14
    ```

    `--mark-done` runs nothing and records `[start, end)` as given. It is safe to repeat.

4. Run as usual. A backfill over the seeded range reruns zero windows. The next `pipeloom run`
   starts at the first window the ledger does not cover, the one that holds the watermark, plus
   the `lookback` windows just before it. Those windows run in full and replace their rows, so the rows
   the watermark model appended to them are not duplicated.

`--end` can also be the watermark itself. A window the ledger covers only in part is not
covered, so the window that holds the watermark still runs in full. Never set `--end` at or past
the end of that window: whole windows the watermark model never loaded would count as done, and
their rows would come back only with `pipeloom backfill --rerun all` over them.
