# ADR-0054: Interval incrementals and the interval ledger

Architecture: [Section 9, Write strategies and idempotency](../architecture.md)

Amends [ADR-0032](0032-delta-writes-and-idempotency.md). [ADR-0008](0008-incremental-and-state.md) and [ADR-0027](0027-incremental-models.md) describe the watermark incrementals, which stay as they are.

## Context

A watermark incremental reads the rows after the largest value it has seen. It cannot reprocess one past day, rerun a gap, or tell which days it has processed. Late data needs a lookback, and a lookback with append writes duplicates (PLP410). The 1.3 roadmap asks for windows of event time, a ledger of the windows a model has processed, and writes that replace one window without touching the rest of the table. The plugin API may only grow additively (ADR-0016, amendment 1.1): no new member on `EngineSession` or `StateStore`.

## Options

- **Reuse `replace_partitions` for the window write.** No new write field. But a table partitioned by month cannot replace one day, an empty window replaces nothing and leaves stale rows, and a table partitioned differently loses rows outside the window.
- **An explicit `replace_range` on `WriteConfig` under a capability.** The engine deletes the rows of one half-open range and inserts the batch. An empty window deletes its old rows, and rows outside the range are never touched, whatever the partitioning.
- **New optional `StateStore` methods for the ledger.** Clean, but every store needs code, and the Delta store needs a new table.
- **A reserved key in the existing watermark records.** The compare-and-swap that stores watermarks also guards the ledger, so every store holds it without new code.

## Decision

Config. `incremental.interval` is optional and additive: `event_time` (column), `grain` (`hour`, `day`, `week`, `month` or `year`), `begin` (an ISO date or date-time; naive means UTC) and `lookback` (a number of windows, default 0). Windows are half-open UTC ranges `[start, end)`. A window starts at a grain boundary in UTC: the start of the hour, midnight, Monday midnight, the first of the month, or January 1. The first window is the one that holds `begin`, and the last is the one that holds the run's start time, so the current, partial window runs too. A time zone offset in `begin` is converted to UTC, so a daylight saving change never stretches or shrinks a window. While `interval` is unset it is absent from the manifest and the checksums, so 1.2 plans keep their hash.

Validation. L4 reports PLP417 when an interval model is not materialized incremental, writes with a strategy other than append, also sets `incremental.column`, or declares a contract without the event_time column. L5 reports PLC514 when the engine does not declare `interval_writes`. A missing event_time column at run time is PLM209 before anything is read in full or written.

Plugin API (1, 1). `EngineCapabilities.interval_writes: bool = False`. `WriteConfig.replace_range: ReplaceRange | None` (column, start, end) applies with the append strategy: the engine deletes the target rows whose column falls in the range and inserts the batch, in one atomic step where it can. A batch row outside the range, or with a null column, is a `WriteKeyError` (PLM206). The input filter is an optional member in its own protocol, `SupportsFilterBetween.filter_between(rel, column, start, end)`, reached through the core helper `pipeloom.engine.protocols.filter_between`, whose default is a Narwhals filter. A (1, 0) plugin keeps loading: its minor is not above the core's.

Engines. polars and pandas share the filesystem catalog writer: it reads each committed file, rewrites the files that hold rows in the range without those rows, and adds the batch, all in one commit. DuckDB runs `DELETE` and `INSERT` in the transaction that records the token. Spark and Delta keep `interval_writes=False` until a later 1.3 change.

Runner. An interval model is one execution unit, so the DAG, the unit graph and the renderers do not change. The unit computes its windows and runs them first to last. Each window reads the incremental input filtered to the window, runs the body with `ctx.interval` set to `(start, end)`, checks quality and the contract, and writes with `replace_range` over the window bounds, converted to the event_time column's type (aware for a zoned timestamp, naive UTC for a plain timestamp, a date for a date column; hour grain needs a timestamp). A failure stops the unit at that window; the earlier windows stay committed. A full refresh runs every window from `begin`, and its first window overwrites the table.

Idempotency (amends ADR-0032). Each window has its own token `run_id:model:interval_id`. The interval id is deterministic: `<start>_<end>` as `YYYYMMDDTHHMMSSZ`. A retry of the unit in the same run finds the tokens of the windows it already wrote and skips them, so the second window of a run is never mistaken for the first. On Delta each window will get its own transaction version when Spark gains interval writes. The run_results row keeps its unit `idempotency_token` and gains an optional `intervals` array: one entry per window with `interval_id`, `start`, `end`, `idempotency_token`, `skipped` and `rows_written`. Rows of other models do not carry it, and the frozen 1.0 required set is unchanged.

Ledger. The processed time of a model is stored as a watermark record under the reserved key `<model>@intervals` (model names are identifiers, so it never names a model), with the dtype `pipeloom.intervals/1` and a value that is a list of merged `[start, end]` ISO-string pairs. The state codec does not convert datetimes nested in a list, so the pairs are text. After each window the runner merges `[start, min(end, run start)]` in with a compare-and-swap loop: a concurrent writer makes it read and merge again, so no interval is lost. The key carries the target, so a dev target never reads or writes the prod ledger. The Delta store encodes values through the same codec, so the round trip holds there by code reading; the state contract suite covers sqlite and json.

Which windows run. Without a ledger, or on a full refresh, every window from `begin` to now. Otherwise the windows whose time up to the run start the ledger does not cover, plus the `lookback` windows just before the first of them. The partial window of the previous run is covered only up to that run's start, so the next run processes it again in full. When nothing is missing, nothing runs and the row reports zero rows written.

Left for later 1.3 changes, with hooks in place: Spark and Delta interval writes, the outcome fields of ADR-0056 and `on_cursor_value_missing`. Backfill, restate and `--mark-done` follow in the Backfill section.

### Backfill (amendment, 1.3)

`pipeloom backfill --select ... --start ... --end ... [--rerun missing|all] [--on-failure stop|continue] [--mark-done]` is `pipeloom run` with an explicit range. It reuses the run's flags (not `--full-refresh`, `--manifest`, `--state` or `--defer`), and `RunOptions.backfill` carries the range to the runner, to `RuntimeContext.backfill` and, through a hidden `--backfill JSON` flag, to the child processes of the task boundary. The `args` of run_results record it under `backfill`.

- Range. The bounds are ISO dates or date-times, naive meaning UTC, as `begin`. A model runs the windows of its grain that overlap `[start, end)`, so bounds off a window edge round outward: a backfill never leaves part of a window it touches unprocessed. The range is clipped to `begin` and to the run start. `lookback` does not apply, since the range is explicit.
- `--rerun missing` (default) runs the windows the ledger does not cover, with the same coverage test as a run. Interval ids and tokens are unchanged, so a backfill and a run of the same window share its id.
- Restate is `--rerun all`, not a subcommand: one command with one more value is the smaller surface. It first removes the rounded range from the ledger (the same compare-and-swap loop, subtracting instead of merging), then runs as `missing`. Removing first means a restate that stops halfway leaves exactly the windows it did not redo as missing, so `--rerun missing` or `pipeloom retry` finishes it. A retry of a backfill run inherits its range with `rerun` set to `missing` for that reason.
- `--on-failure stop` (default) stops a model at its first failing window, as a run does. `continue` runs every window, then fails the unit with the first error's code and a message that lists every failed window. Failed windows get the optional `failed: true` in their `intervals` entry, in both modes.
- `--mark-done` records `[start, min(end, run start))` in the ledger as given, without rounding, and runs nothing: it never claims time the user did not name. Merging an interval the ledger holds changes nothing, so it is safe to repeat.
- Selection. Selectors work as in run: a selected model without `interval` runs as in run, so `+` brings descendants. A selection with no interval model, and `--mark-done` with any selected model that has no interval (it would have to run), stop with PLX132 (exit 2) before a run id is used.
- A backfill is never a full refresh (`RunOptions` refuses both).

Provenance of the amendment: designed from the roadmap item and the behavior of the interval runner above. No code from other projects was used.

Provenance: designed from the public dbt microbatch documentation (window rounding and lookback behavior) and from the behavior of pipeloom 1.2. No code from other projects was used.

## Consequences

A model can be rerun for a gap and reprocesses late data per window without duplicates. Window writes on the filesystem catalog read every file of the table; per-file statistics would let them skip files, which is a later optimization. A run that starts many windows does one write and one ledger update per window. The `intervals` array grows with the number of windows in a run.

## Status

Accepted

## Date

2026-11-21
