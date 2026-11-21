# Quickstart

This walks through a small project with one source and two models, then runs it on polars.

## Install

```console
$ pip install "pipeloom[polars]"
```

Use `pipeloom[pandas]` for the pandas engine and `pipeloom[duckdb]` for DuckDB. Add the `sqlglot` extra to get warnings about SQL
that reads tables you did not declare (`pipeloom[polars,sqlglot]`).

## Try it in one command

With the DuckDB engine installed, one command writes a sample project, builds it and writes the
docs site, with nothing else to set up:

```console
$ uvx --from "pipeloom[duckdb]" pipeloom demo
```

It prints the path of `target/docs/index.html` in the new `pipeloom_demo` folder and the commands
to try next. See [pipeloom demo](reference/cli.md#pipeloom-demo).

<figure markdown="span">
  ![pipeloom demo builds a DuckDB project in one command; by hand, pipeloom init, then validate, then run or build. Both write DuckDB tables and run artifacts, and the docs site is built from the artifacts.](diagrams/quickstart.light.svg#only-light)
  ![pipeloom demo builds a DuckDB project in one command; by hand, pipeloom init, then validate, then run or build. Both write DuckDB tables and run artifacts, and the docs site is built from the artifacts.](diagrams/quickstart.dark.svg#only-dark)
  <figcaption>Two ways in: the demo, or init, validate and run. Source: <a href="https://github.com/emanuel-luis/pipeloom/blob/main/docs/diagrams/quickstart.d2">quickstart.d2</a>.</figcaption>
</figure>

## Start from `pipeloom init`

The fastest start is to let pipeloom write the project:

```console
$ pipeloom init shop
Project name [shop]:
Engine (polars, pandas, spark, databricks, duckdb) [polars]:
Created project 'shop' (polars) in shop
Next steps:
  cd shop
  pipeloom validate
  pipeloom run
```

It writes `pipeloom.yml` with a `dev` target, a source group and a `models:` tree, one decorated
model in each of `models/bronze`, `models/silver` and `models/gold`, a properties file
`models/silver/_properties.yml`, a small CSV in `data/`, a `.gitignore` and a README. Pass
`--no-input` (and `--name`, `--engine`, `--catalog`) to skip the questions; see
[pipeloom init](reference/cli.md#pipeloom-init).

Every YAML file starts with a schema modeline, and `.vscode/settings.json` maps the published
schemas to the project files. With only the YAML extension (`redhat.vscode-yaml`, recommended in
`.vscode/extensions.json`), VS Code completes keys and flags mistakes as you type.

### Add a model

```console
$ pipeloom new model customers_clean --folder silver
models/silver/customers_clean.py
$ pipeloom validate
```

`pipeloom new model NAME` writes one `.py` file under `models/` with a function decorated with
`@model`. By default its body returns `ctx.sql("select 1 as id")`, or, when the default target is
pandas, a frame stub; pass `--sql` or `--python` to choose. Add a parameter for each input,
named like the model or source table it reads, then fill in the body. See
[pipeloom new](reference/cli.md#pipeloom-new-model).

The rest of this page writes a project by hand, to show each part.

## Write the project

`pipeloom.yml`:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/project.json
config_version: 1
name: shop
sources:
  raw:
    format: csv
    location: raw
    tables:
      orders:
        location: orders.csv
targets:
  dev:
    engine:
      kind: polars
    catalog:
      kind: filesystem
      root: target/warehouse
    state:
      kind: json
      path: .pipeloom/state
```

`raw/orders.csv`:

```text
id,customer_id,amount
1,10,5.0
2,10,2.5
3,11,7.0
```

`models/orders.py`:

```python
from pipeloom import ModelContext, model


@model(tags=["staging"])
def stg_orders(ctx: ModelContext, orders):
    return ctx.sql("select id, customer_id, amount from orders")


@model
def orders_by_customer(ctx: ModelContext, stg_orders):
    return ctx.sql("select customer_id, sum(amount) as total from stg_orders group by customer_id")
```

A model is a function marked with `@model`. Its parameters are its inputs: `orders` binds to the
source table `raw.orders`, and `stg_orders` to the model of that name. `ctx.sql` runs a query over
the inputs by parameter name and returns the engine's frame; a model may also build and return a
frame itself. Settings such as `tags` are `@model` keywords, or folder defaults in a `models:`
tree in `pipeloom.yml`. Coming from 0.8 and YAML models? See
[Migrating from 0.8](guide/migrate-0.9.md).

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
