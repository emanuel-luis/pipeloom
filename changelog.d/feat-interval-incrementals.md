### Added

- Interval incrementals: `incremental.interval` with `event_time`, `grain`, `begin` and `lookback` runs a model in half-open UTC windows, each replacing its own range of the target, and records the processed windows in an interval ledger in the state store ([ADR-0054](docs/adr/0054-interval-incrementals.md), [guide](docs/guide/incremental.md#interval-models)).
- Plugin API (1, 1): the capability `interval_writes`, `WriteConfig.replace_range`, and the optional `SupportsFilterBetween.filter_between` reached through `pipeloom.engine.protocols.filter_between`. polars, pandas and DuckDB declare `interval_writes`; plugins built for (1, 0) keep loading.
- `ctx.interval`: the UTC window an interval model is running.
- The optional `intervals` array on run_results rows of interval models, one entry per window with its own idempotency token.
- Diagnostic codes PLP417 (interval options conflict), PLC514 (engine has no interval writes) and PLM209 (interval cannot be used).
- Engine contract cells for `replace_range` and state contract cells for the interval ledger.
