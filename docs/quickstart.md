# Quickstart

This walks through a small project with one source and two models, then runs it on polars.

## Install

```console
$ pip install "pipeloom[polars]"
```

Use `pipeloom[pandas]` for the pandas engine. Add the `sqlglot` extra to get warnings about SQL
that reads tables you did not declare (`pipeloom[polars,sqlglot]`).

## Write the project

`pipeloom.yml`:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/project.json
config_version: 1
name: shop
targets:
  dev:
    engine: {kind: polars}
    catalog: {kind: filesystem, root: target/warehouse}
    state: {kind: json, path: .pipeloom/state}
```

`raw/orders.csv`:

```text
id,customer_id,amount
1,10,5.0
2,10,2.5
3,11,7.0
```

`models/orders.yml`:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/models.json
sources:
  - {name: raw_orders, format: csv, location: raw/orders.csv}
models:
  - name: stg_orders
    sql: select id, customer_id, amount from raw_orders
    inputs: {raw_orders: {source: raw_orders}}
    tags: [staging]
  - name: orders_by_customer
    sql: select customer_id, sum(amount) as total from stg_orders group by customer_id
    inputs: {stg_orders: {ref: stg_orders}}
```

## Check it

```console
$ pipeloom validate
Project 'shop' is valid.
```

`validate` runs every layer from YAML syntax to the capability check of your engine. Findings
print to stderr with a code such as `PLR201`; the [error codes](reference/errors.md) page explains
each one.

## Explore the graph

```console
$ pipeloom ls --select tag:staging
stg_orders
$ pipeloom graph
stg_orders
orders_by_customer <- stg_orders
```

## Run it

```console
$ pipeloom run
... 1/2 OK     stg_orders ............. table/overwrite        3 rows ...
... 2/2 OK     orders_by_customer ..... table/overwrite        2 rows ...
```

Each model is now a table under `target/warehouse/main/`. Rerun with the same `--run-id` and the
data stays byte-identical. See [Running a pipeline](guide/running.md).

## Read the results

```console
$ ls target
manifest.json  run_results.json  runs  warehouse
```

`run_results.json` has one row per model with its status, timing, rows and bytes written and
schema hash; the manifest lists every model, source and execution unit, with a `plan_hash` that
changes when the pipeline changes. See [Run artifacts](reference/artifacts.md).
