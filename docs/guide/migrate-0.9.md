# Migrating from 0.8 to 0.9

In 0.9 every model is a Python function marked with `@model`. YAML is still where the project
lives (`pipeloom.yml`) and where you describe models (properties files), but a YAML file no longer
declares a model. There is no deprecation release and no `pipeloom migrate` command: you rewrite
each model once, by hand, with the mapping below. The decision is
[ADR-0050](../adr/0050-python-first-authoring.md).

## What changed

| 0.8 | 0.9 |
|---|---|
| A model entry with `sql:` in a model file | A function that returns `ctx.sql(...)` |
| A model entry with `python: module:function` | The same function, decorated with `@model` |
| `inputs:` on the model entry | The function's parameters, bound by name, or `inputs=` |
| Model settings on the model entry | `@model(...)` keywords, or `config:` in a properties entry |
| `description:` and `columns:` on the model entry | `@model(...)` keywords, or a properties entry |
| A flat `sources:` list in a model file | Source groups under `sources:` in `pipeloom.yml` |
| `_defaults.yml` in a folder | `+key` settings for that folder in the `models:` tree |
| `defaults.models` in `pipeloom.yml` | Root `+key` settings of the `models:` tree |
| `models.json` and `defaults.json` schemas | `properties.json` (and `project.json`, unchanged) |
| A manifest body with `kind: sql` | Every body is `kind: python`; recompile old manifests |

What fails, and how:

- A properties entry with `sql`, `python` or `inputs`, a `sources:` list in a properties file and
  a `defaults:` block in `pipeloom.yml` are PLS101 (unknown field), with a hint to this page.
- A model setting left at the top of a properties entry (`materialized`, `tags`, `retries` and the
  like) is PLS101 too. A properties entry takes only `name`, `description`, `config`, `columns`,
  `data_tests` and `unit_tests`, so the setting moves under `config:`.
- A `_defaults.yml` file is now read as a properties file, so its `models:` mapping is PLS102.
- A manifest written by 0.8 does not load (PLX107). Compile it again with 0.9.
- PLK601, PLK604, PLC502 and PLR205 are retired: the checks they made have no subject left.

Plan hashes and node checksums change once, so the first `state:modified` selection after the
upgrade selects every model.

## Keyword mapping

Every key a YAML model entry accepted is a keyword of `@model`, with the same name and the same
values. Nested settings take a plain dict with the YAML keys, or the typed classes `Write`,
`Incremental`, `Column`, `Expectation`, `Quality` and `Deletes`.

| YAML key on the model entry | `@model` keyword |
|---|---|
| `name` | `name=` (defaults to the function name) |
| `description` | `description=` |
| `materialized` | `materialized=` |
| `inputs: {alias: {ref: m}}` | a parameter named `alias` bound to model `m`: `inputs={"alias": ref("m")}`, or just a parameter named `m` |
| `inputs: {alias: {source: s}}` | `inputs={"alias": source("group", "table")}`, or a parameter named like the table |
| `incremental` | `incremental=Incremental(...)` or a dict |
| `write` | `write=Write(...)` or a dict |
| `partition_by`, `cluster_by`, `tags` | the same keyword, a list |
| `columns` | `columns=[Column(...), ...]` or a list of dicts |
| `expectations` | `expectations=[Expectation(...), ...]`; checks with `not_null`, `unique`, `accepted_values`, `between`, `regex`, `row_count`, `freshness` |
| `quality` | `quality=Quality(...)` or a dict |
| `retries`, `retry_delay`, `retry_on`, `timeout` | the same keyword |
| `frame_api`, `sql_dialect`, `schema`, `catalog`, `meta` | the same keyword |
| `${var:name}` in a setting | `var("name", default=...)` |

## Step by step

1. Move flat sources into source groups in `pipeloom.yml` (see below). The unique id of a source
   becomes `source.<project>.<group>.<table>`.
2. Replace each `_defaults.yml` and `defaults.models` with `+key` settings in the `models:` tree.
3. For each model entry, write a function in a `.py` file under `model_paths` and move its
   settings to `@model(...)`. A `python:` model keeps its function; add the decorator where it
   is defined and make sure the file lies under `model_paths` and a `python_paths` root.
4. Keep `description`, `columns` and other documentation in a properties file if you prefer
   YAML for them, as a `models:` list of entries without `sql`, `python` or `inputs`. Settings such
   as `materialized` or `tags` go under `config:` in the entry; at the top of an entry they are
   PLS101.
5. Update the schema modelines: properties files use `properties.json`; drop `models.json` and
   `defaults.json`.
6. Run `pipeloom validate`, then `pipeloom compile` to write a new manifest.

## SQL models

A SQL model becomes a function that returns `ctx.sql(...)`. The query reads the inputs by their
parameter names.

Before, in `models/silver/silver.yml`:

```yaml
models:
  - name: orders_clean
    sql: select * from orders where o_totalprice > 0
    inputs:
      orders:
        source: raw_orders
    tags: [silver]
```

After, in `models/silver/orders_clean.py`:

```python
from pipeloom import ModelContext, model, source


@model(inputs={"orders": source("tpch", "orders")}, tags=["silver"])
def orders_clean(ctx: ModelContext, orders):
    return ctx.sql("select * from orders where o_totalprice > 0")
```

When the parameter has the name of a model, or of a table in exactly one source group, it binds
without `inputs=`: `def orders_clean(ctx, orders)` reads the source table `orders` on its own.
`ctx` is optional; take it only when the function needs `ctx.sql` or the run context.

The query is written in the engine's dialect. Set `sql_dialect` (a keyword, or `+sql_dialect` in
the tree) to write it in another dialect; pipeloom translates it with sqlglot, which is the
`pipeloom[sqlglot]` extra. pandas had no SQL in 0.9: write those models with frames, or, from 1.2,
install `pipeloom-pandas[sql]`, which runs `ctx.sql` through DuckDB.

## Views

A view is a function that returns `ctx.sql(...)` with `materialized="view"`. The runner calls it
in view mode and stores the query, so the function may not use its input frames for anything
other than passing them to `ctx.sql`, and `ctx.sql` may not take `inputs=` (PLX120).

```python
@model(materialized="view")
def active_customers(ctx, customers):
    return ctx.sql("select * from customers where active")
```

## Python models

Before:

```yaml
models:
  - name: enriched
    python: shop_models:enriched
    inputs:
      orders:
        ref: orders_clean
    retries: 2
```

After, in the same module:

```python
from pipeloom import model


@model(retries=2)
def enriched(orders_clean):
    ...
```

The parameter `orders_clean` binds to the model of that name. To keep the old alias, write
`inputs={"orders": ref("orders_clean")}` and name the parameter `orders`. Inputs are passed by
keyword, so `*args` and positional-only parameters are PLK602.

## Properties and the `models:` tree

Settings resolve, highest first: the decorator, the model's properties entry (its `config`), the
`models:` tree from the model's folder up to the root, the root `+keys`, the built-in defaults.

Before, `models/silver/_defaults.yml` and `pipeloom.yml`:

```yaml
models:
  schema: silver
  tags: [silver]
```

```yaml
defaults:
  models:
    materialized: table
```

After, in `pipeloom.yml`:

```yaml
models:
  +materialized: table
  silver:
    +schema: silver
    +tags: [silver]
```

A properties file is any `*.yml` or `*.yaml` under `model_paths`:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/properties.json
models:
  - name: orders_clean
    description: Orders with a positive total.
    config:
      retries: 2
    columns:
      - name: o_orderkey
        dtype: int64
        nullable: false
```

Decorator `columns` replace properties `columns` whole.

## Sources

Before, in a model file:

```yaml
sources:
  - name: raw_orders
    format: parquet
    location: data/tpch/orders.parquet
  - name: raw_customer
    format: parquet
    location: data/tpch/customer.parquet
```

After, in `pipeloom.yml`:

```yaml
sources:
  tpch:
    format: parquet
    location: data/tpch
    tables:
      orders:
        location: orders.parquet
      customer:
        location: customer.parquet
```

A table `location` is joined to the group `location`; a table `table` is qualified by the group
`schema`. A table may set one of `location` and `table`, not both (PLS108). A target can override
a group under `targets.<name>.sources.<group>`, and `null` removes a key.
