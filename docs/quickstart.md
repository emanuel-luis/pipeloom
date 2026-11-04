# Quickstart

This walks through a small project with one source and two models. In 0.1 pipeloom validates
and compiles projects; running them comes in 0.2.

## Install

```console
$ pip install pipeloom
```

## Write the project

`pipeloom.yml`:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/project.json
config_version: 1
name: shop
engine: {kind: memory}
targets:
  dev: {}
```

`models/orders.yml`:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/models.json
sources:
  - {name: raw_orders, format: memory, location: raw/orders}
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

## Compile the plan

```console
$ pipeloom compile
target/manifest.json
```

The manifest lists every model, source and execution unit, with a `plan_hash` that changes when
the pipeline changes. Compiling the same project twice gives the same hash.
