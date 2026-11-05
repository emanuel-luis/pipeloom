# Spark engine

`pipeloom-spark` runs models on [Apache Spark](https://spark.apache.org) DataFrames and Spark SQL.
This release reads sources, runs SQL and Python models and ephemeral models, and cancels running
units. Writing Delta tables (the write strategies, idempotent writes, views and intermediates)
arrives in a later release, so a target on this engine cannot materialize tables yet.

```console
$ pip install "pipeloom[spark]"      # local Spark: pulls pipeloom-spark[local]
$ pip install pipeloom-spark         # Databricks, Databricks Connect, or any environment with pyspark
```

```yaml
targets:
  dev:
    engine: {kind: spark, mode: local, conf: {spark.sql.shuffle.partitions: "4"}}
    catalog: {kind: delta_path, root: warehouse}
```

## Installation

pyspark is not a hard dependency. Databricks runtimes and Databricks Connect ship their own
pyspark, and installing another one breaks them, so `pipeloom-spark` depends only on `pipeloom`.
The extra `local` installs `pyspark[connect]` (4.0.1 up to, not including, 4.3) and `delta-spark`
(4.1 up to, not including, 4.5). A thin Spark Connect client may install `pyspark-client`
instead, never together with `pyspark`, since both own the `pyspark` package.

Local Spark needs Java 17 or 21. Spark 4 runs with ANSI mode on by default: an invalid cast fails
instead of giving null. On Windows, run Spark in WSL; Windows Python workers do not work without
winutils.

## Session modes

| Mode | Behavior |
| --- | --- |
| `local` | Builds a session with `master` (default `local[*]`), `app_name` and `conf`, and adds the Delta jars, SQL extension and catalog unless `delta: false`. |
| `connect` | Builds a Spark Connect session from `remote` or `SPARK_REMOTE`. A URL that starts with `local` starts a local Connect server with the same conf. |
| `active` | Uses the existing session and never builds, configures or stops anything. |
| `auto` (default) | `active` when `DATABRICKS_RUNTIME_VERSION` is set or a session exists, else `connect` when `remote` or `SPARK_REMOTE` is set, else `local`. |

Only a session the engine built is stopped. The engine sets no Spark conf on a session it did not
build, and sets none at run time on any session: `conf` applies when the session is built, and
`conf` with mode `active` is an error. Classic and Connect cannot share one Python process.

`boundary: task` with local Spark starts one JVM per unit; it works, and is slow. Intermediates on
a remote Connect server need an artifacts root that client and server share.

## Catalogs

- `delta_path`: Delta tables at `<root>/<schema>/<name>`. No metastore, and it works across
  processes. It cannot hold views.
- `spark_catalog`: tables and views by name through the session catalog (`catalog` option, else
  the current catalog). It is the base the Unity Catalog support of a later release reuses.

## Capabilities

| Capability | Value |
| --- | --- |
| Source formats | `delta`, `parquet`, `csv`, `json`, `ndjson` |
| Materializations | `table`, `ephemeral` (no write strategy yet) |
| SQL models | yes, inputs bound by alias |
| Cancel | yes, through Spark job tags |
| Portable frames (`frame_api: narwhals`) | yes |

`csv` reads a header and infers types; `json` is one multi-line document and `ndjson` is one
document per line. Reader options pass through as strings.

## SQL

A SQL model's inputs are bound without session temp views: each alias becomes a common table
expression over a DataFrame passed to `spark.sql`, so two units can use the same alias at the
same time and an alias shadows a catalog table of the same name.

## Cancellation

Each unit adds a job tag (`pipeloom:<run id>:<unit id>`) for the length of its reads, body and
write. `cancel` interrupts the jobs with that tag (`SparkContext` job tags on classic Spark, session tags on Connect) and keeps interrupting until the unit ends.
Cancellation is cooperative: Python code between jobs is not stopped.

## Timestamps

The engine reads timestamps as microseconds since the epoch, so the Python process time zone
never changes a value. User code that calls `collect()` on a classic session gets naive local
datetimes for timestamp columns.
