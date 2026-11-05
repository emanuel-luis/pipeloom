# pipeloom examples

One TPC-H transformation, written four times: on polars, pandas, Spark and Databricks. Each project
reads the same five TPC-H tables and builds the same bronze, silver and gold layers, so you can
compare how a pipeloom project looks on each engine.

| Project | Engine | Models | Output |
|---|---|---|---|
| `tpch_polars` | polars | SQL | parquet under `target/warehouse` |
| `tpch_pandas` | pandas | Python (pandas has no SQL) | parquet under `target/warehouse` |
| `tpch_spark` | Spark, local mode | SQL | Delta under `target/warehouse` |
| `tpch_databricks` | Spark, serverless job | SQL | Unity Catalog tables |

## Layout

Each layer lives in its own catalog, and every model uses the schema `tpch`. A layer folder's
`_defaults.yml` sets the catalog, and the target's naming sets the schema.

| Layer | Catalog | Tables | Columns |
|---|---|---|---|
| bronze | `pipeloom_bronze` | `orders`, `lineitem`, `customer`, `nation`, `region` | the original TPC-H columns |
| silver | `pipeloom_silver` | `orders_enriched` | `order_key, customer_key, customer_name, market_segment, nation, region, order_date, order_status, total_price` |
| silver | `pipeloom_silver` | `lineitem_revenue` | `order_key, line_number, quantity, extended_price, discount, net_revenue, ship_date` |
| gold | `pipeloom_gold` | `revenue_by_region_month` | `region, nation, order_year, order_month, orders, net_revenue` |
| gold | `pipeloom_gold` | `top_customers` | `customer_key, customer_name, nation, region, orders, net_revenue` |

`net_revenue` of a line item is `l_extendedprice * (1 - l_discount)`. Gold rounds the sums to 2
decimals. `top_customers` keeps the ten customers with the most net revenue, ties broken by
customer key. Silver and gold declare a few expectations (not null and unique keys, a non-empty
gold table), which fail the run when broken.

## Where the output lands

On the path catalogs (polars, pandas, Spark) a table is a directory,
`target/warehouse/<catalog>/tpch/<table>/`, for example
`target/warehouse/pipeloom_gold/tpch/top_customers/`. On Databricks it is a Unity Catalog table,
`pipeloom_gold.tpch.top_customers`.

## Prerequisites

- Python 3.11 or newer and [uv](https://docs.astral.sh/uv/).
- Spark: Java 17 or 21 on Linux or WSL (Spark does not run in Windows Python).
- Databricks: a workspace with Unity Catalog and a profile in your `~/.databrickscfg`.

## Generate the data

```console
$ uv run examples/tpch_data.py
```

This writes the five tables as parquet to `examples/data/tpch/` (scale factor 0.01, about 15,000
orders). Options: `--sf` and `--out`. DuckDB generates the data and is installed only for this
script, in its own inline environment. The folder is git-ignored.

## Run on polars and pandas

```console
$ uv sync --all-packages
$ uv run pipeloom run --project-dir examples/tpch_polars --target dev
$ uv run pipeloom run --project-dir examples/tpch_pandas --target dev
```

To use a project outside this checkout, copy its folder, install `pipeloom[polars]` or
`pipeloom[pandas]`, and run `pipeloom run --project-dir <folder> --target dev`. Relative source locations resolve against the project directory. Point a project at other data
with `--vars '{"tpch_dir": "/path/to/tpch"}'`.

## Run on Spark

`tpch_spark` runs a local Spark session with the Delta jars, so it needs Linux or WSL with Java 17
or 21 and `pipeloom[spark]`:

```console
$ pipeloom run --project-dir examples/tpch_spark --target dev
```

An Astro project that runs it inside a container will be added in a later PR.

## Run on Databricks

The project expects these objects to exist: the catalogs `pipeloom_bronze`, `pipeloom_silver` and
`pipeloom_gold`, the schema `pipeloom_bronze.ops` and the volume
`pipeloom_bronze.ops.artifacts`. pipeloom creates schemas but never catalogs. On workspaces with
Default Storage, create the catalogs with SQL, because the catalogs REST API needs a managed
location there:

```sql
CREATE CATALOG IF NOT EXISTS pipeloom_bronze;
CREATE CATALOG IF NOT EXISTS pipeloom_silver;
CREATE CATALOG IF NOT EXISTS pipeloom_gold;
CREATE SCHEMA IF NOT EXISTS pipeloom_bronze.ops;
CREATE VOLUME IF NOT EXISTS pipeloom_bronze.ops.artifacts;
```

The sources read the workspace's `samples.tpch` tables, so no data upload is needed. Then:

```console
$ pipeloom databricks --profile <your-profile> deploy --target dev --project-dir examples/tpch_databricks
$ pipeloom databricks --profile <your-profile> run --target dev --pipeline main --project-dir examples/tpch_databricks
```

The job installs `pipeloom`, `pipeloom-spark` and `pipeloom-databricks` 0.7.0 on serverless
compute, and keeps run state in `pipeloom_bronze.ops._pipeloom_state_tpch`.

## Differences between the projects

- The SQL of the polars, Spark and Databricks projects is the same text. Only the sources differ:
  parquet files for polars and Spark, `samples.tpch.<table>` for Databricks.
- pandas has no SQL engine (PLC502), so `tpch_pandas` writes the same logic as Python models in
  `src/tpch_pandas/`.
- The tests only validate and compile the Spark and Databricks projects; the parity test below
  runs polars and pandas.

## Tests

`tests/examples/` checks that every project validates and compiles, with no data, Spark or network
needed. The parity test runs the polars and pandas projects on the generated data and compares
both gold tables (same columns, same rows, floats within one cent, because engines add in a
different order):

```console
$ make examples-data
$ make examples-test
```
