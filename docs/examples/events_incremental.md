# Late-arriving events

Clickstream events arrive in daily files, and some of them arrive late: an event stamped 23:15
yesterday shows up in today's file. This example loads them with an incremental model that keeps
a watermark, re-reads a short window to catch the late rows, and merges on the event id so a
resent event never counts twice. It runs on DuckDB and on polars, and a test runs every step on
both in CI.

The project is in `examples/events_incremental`.

## The project

Three models and one source. The source reads every CSV that has landed in `raw/events`.

```yaml
--8<-- "examples/events_incremental/pipeloom.yml"
```

Bronze reads all the files and types the columns. It is a table, rebuilt on each run.

```python
--8<-- "examples/events_incremental/models/bronze/bronze.py"
```

Silver is the incremental model.

```python
--8<-- "examples/events_incremental/models/silver/silver.py"
```

- The watermark is the largest `event_ts` seen so far. A run reads rows with `event_ts` at or
  after the watermark minus two hours, so an event up to two hours behind the newest one still
  gets in.
- The write strategy is `merge` on `event_id`. A row for a known id updates it, a row for a new
  id inserts it. With `append` the resent event would be a second row.
- The batch can hold the same event twice, once from the window and once from the new file. A
  merge fails on a repeated key (PLM206), so the model collapses exact copies with `select
  distinct` first.
- The `update_condition` leaves a matched row alone unless a value changed. That is what lets a
  rerun write nothing even though it reads the window again.

Gold counts events per day from silver.

```python
--8<-- "examples/events_incremental/models/gold/gold.py"
```

## The data

Day 1 is 12 events on 2026-03-01, the last at 23:50.

```text
--8<-- "examples/events_incremental/data/day1/events_2026-03-01.csv"
```

Day 2 has 10 rows. Five are new events on 2026-03-02. The other five are on 2026-03-01 and show
the cases that matter:

- e013, e014 and e015 are late but newer than 21:50 (the watermark minus two hours), so they are
  inside the window.
- e011 is a resend of a day 1 event, identical to the original.
- e016 is stamped 10:00, far outside the window.

```text
--8<-- "examples/events_incremental/data/day2/events_2026-03-02.csv"
```

## Run it

From a checkout of pipeloom, with `uv sync --all-packages` done. The `raw/events` folder stands
in for the place where the files land.

```console
$ mkdir -p examples/events_incremental/raw/events
$ cp examples/events_incremental/data/day1/*.csv examples/events_incremental/raw/events/
$ uv run pipeloom run --project-dir examples/events_incremental --target duckdb
```

Use `--target polars` to run the same project on polars. The numbers below are the same on both.

## Read the result

Day 1. The first run builds everything and sets the watermark to the latest event.

```text
1/3 OK     events ......... table/overwrite       12 rows
state.watermark_committed {'column': 'event_ts', ..., 'before': None, 'after': '2026-03-01T23:50:00', ...}
2/3 OK     events_clean ... incremental/merge     12 rows
3/3 OK     daily_events ... table/overwrite        1 rows
```

Day 2. Copy the second file and run again.

```console
$ cp examples/events_incremental/data/day2/*.csv examples/events_incremental/raw/events/
$ uv run pipeloom run --project-dir examples/events_incremental --target duckdb
```

```text
1/3 OK     events ......... table/overwrite       22 rows
state.watermark_committed {'column': 'event_ts', ..., 'before': '2026-03-01T23:50:00', 'after': '2026-03-02T14:00:00', ...}
2/3 OK     events_clean ... incremental/merge      8 rows
3/3 OK     daily_events ... table/overwrite        2 rows
```

Bronze read all 22 rows, but silver wrote 8: the three late events and the five new ones. The
resent e011 matched its row and changed nothing. The silver table now has 20 rows. The watermark
moved to the newest event, 2026-03-02T14:00:00.

Event e016 is not in silver. It is older than the window, so the incremental run filtered it out
before the merge. A lookback is a bet on how late data can be. Events later than the window need
a wider lookback or a full refresh.

Rerun with no new data.

```text
2/3 OK     events_clean ... incremental/merge      0 rows
```

The watermark stays at 2026-03-02T14:00:00 (`before` and `after` are equal) and silver writes 0
rows. The run still reads the window, but every row matches an identical one. Bronze and gold are
plain tables, so they are rebuilt on every run and report their rows; only the incremental model
skips unchanged data.

Full refresh.

```console
$ uv run pipeloom run --project-dir examples/events_incremental --target duckdb --full-refresh
```

```text
2/3 OK     events_clean ... incremental/overwrite  21 rows
```

A full refresh ignores the watermark, reads all of bronze and overwrites silver. The result has
21 rows, including e016, and the watermark is set again to 2026-03-02T14:00:00. In
`target/run_results.json`, `events_clean` reports `watermark_before` and `watermark_after`,
`is_incremental`, `rows_inserted` and `rows_updated` for each run.

## What to try next

- Set `lookback` to `1d` and start again from day 1. Day 2 now writes 9 rows, because e016 is
  inside the window.
- Delete the `update_condition` and rerun with no new data. Silver now writes 1 row instead of 0:
  e021, the only row inside the window, is updated with the values it already has.
- Switching to `append` is not a one line edit: `unique_key` and `update_condition` do not apply
  to it (PLP405), and with a lookback it writes the window again on every run (PLP410).

See [Incremental models](../guide/incremental.md) and
[Merge, replace_partitions, scd2 and schema changes](../guide/write-strategies.md) for every
option.
