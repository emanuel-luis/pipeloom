### Added

- `incremental.on_cursor_value_missing: raise | include | exclude` for watermark models decides what happens to input rows whose cursor column is null. `raise` fails the unit with the new permanent code PLM210; `include` keeps those rows in every batch (the null never moves the watermark); `exclude` drops them, counts them in the optional `rows_null_cursor` field of run_results and warns. Unset keeps today's behavior ([ADR-0027](docs/adr/0027-incremental-models.md), [guide](docs/guide/incremental.md#rows-with-a-null-cursor)).
- Optional engine session members `count_nulls` and `filter_cursor` in the protocol `SupportsNullCursor`, with Narwhals defaults. DuckDB and Spark implement them natively.
