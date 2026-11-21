# orders_quality

An orders feed from a partner you do not control. The project shows three layers of defence:
expectations that warn, drop or quarantine bad rows, a model contract that stops a schema
break before anything is written, and data tests that judge the table after the write.

Run it on DuckDB (the default target) from the repository root:

```console
$ uv sync --all-packages
$ uv run pipeloom build --project-dir examples/orders_quality
```

That is day 1, a clean batch. Pick another batch with `--vars`, and another engine with
`--target polars`:

```console
$ uv run pipeloom build --project-dir examples/orders_quality --vars '{"batch": "day2"}'
$ uv run pipeloom build --project-dir examples/orders_quality --vars '{"batch": "day3"}'
```

What to look at:

- day 2 prints three WARN lines and writes 5 of 8 rows. `target/run_results.json` has
  `rows_quarantined: 3` and one entry per check in `expectations`. The dropped rows are in the
  table `orders__quarantine`.
- day 3 adds a `coupon` column. The run exits 1 with PLM208 and `orders` keeps day 2 rows.

The full walkthrough is in `docs/examples/orders_quality.md`, and
`tests/examples/test_walkthrough_orders_quality.py` runs it in CI.
