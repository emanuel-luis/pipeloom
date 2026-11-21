# customers_scd2

A customer dimension with history. Three daily CRM snapshots (`data/customers_day1.csv` to
`day3.csv`) load into `dim_customers` with the `scd2` write strategy: a changed name, city or tier
closes the current version and opens a new one, and a customer with status `closed` has its
current version closed.

Run the three days on DuckDB, then print the dimension:

```console
$ uv sync --all-packages
$ for d in 1 2 3; do uv run pipeloom run --project-dir examples/customers_scd2 --vars "{\"day\": \"day$d\"}"; uv run examples/customers_scd2/show.py; done
```

Targets: `duckdb` (the default), `polars` and `pandas` (`--target`, and the same name for
`show.py`). Look at `valid_from`, `valid_to` and `is_current` after each day. `expected/` holds
the rows the walkthrough test expects when each run starts at 2026-01-0N 00:00 UTC (`--run-id` and
`--run-started-at`). The full walkthrough is in `docs/examples/customers_scd2.md`.
