### Documentation

- A new example, [Late-arriving events](docs/examples/events_incremental.md): an incremental model with a watermark, a lookback and a merge on a key, run on DuckDB and polars, with a walkthrough test that asserts watermarks, row counts and an idempotent rerun.
- A `customers_scd2` example and walkthrough: three CRM snapshots build a customer dimension with history through the `scd2` write strategy on DuckDB, polars and pandas, with an as-of query and a walkthrough test.
- A new example, `examples/orders_quality`, and its walkthrough page: an untrusted partner feed with warn, drop and quarantine expectations, a model contract that catches a schema break (PLM208) and data tests, run on DuckDB and polars and tested in CI.
- A new example, `examples/tested_models`, with a walkthrough page: unit tests with inline and CSV rows, data tests, `pipeloom test` and `pipeloom build` on DuckDB and polars, and a plain pytest on a model function. Its CI test checks that a deliberately wrong copy fails with PLX114.
- The docs site gets a light and dark theme that follows the OS, a logo, top tabs, copyable code, click-to-zoom diagrams, an outdated-version banner, canonical URLs on `/latest/` and a root 404 page that redirects to `/latest/`.
