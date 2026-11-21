# events_incremental

Late-arriving clickstream events on an incremental model. Raw events land as daily CSV files. A
bronze model reads them all, a silver incremental model keeps a watermark on `event_ts` with a
two hour lookback and merges on `event_id`, and a gold model counts events per day. It runs on
DuckDB and on polars.

The walkthrough is in the docs page `docs/examples/events_incremental.md`, and
`tests/examples/test_walkthrough_events_incremental.py` runs it in CI.

## Run it

From the repository root, with `uv sync --all-packages` done:

```console
$ mkdir -p examples/events_incremental/raw/events
$ cp examples/events_incremental/data/day1/*.csv examples/events_incremental/raw/events/
$ uv run pipeloom run --project-dir examples/events_incremental --target duckdb
```

Use `--target polars` for the polars engine. Then add day 2 and run again:

```console
$ cp examples/events_incremental/data/day2/*.csv examples/events_incremental/raw/events/
$ uv run pipeloom run --project-dir examples/events_incremental --target duckdb
$ uv run pipeloom run --project-dir examples/events_incremental --target duckdb
$ uv run pipeloom run --project-dir examples/events_incremental --target duckdb --full-refresh
```

## What to look at

- The `state.watermark_committed` event: `before` and `after` of `event_ts`.
- The silver line of each run: 12 rows on day 1, 8 on day 2, 0 on the rerun, 21 on the full
  refresh.
- `target/run_results.json`: `watermark_before`, `watermark_after`, `rows_inserted` and
  `rows_updated` of `events_clean`.

Output goes to `target/`, state to `.pipeloom/` and the landed files to `raw/`. All three are
git-ignored.
