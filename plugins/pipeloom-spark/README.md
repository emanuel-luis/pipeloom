# pipeloom-spark

The [Apache Spark](https://spark.apache.org) engine for
[pipeloom](https://github.com/emanuel-luis/pipeloom).

```console
$ pip install "pipeloom[spark]"          # local Spark: pulls pipeloom-spark[local]
$ pip install pipeloom-spark             # Databricks or an environment that already has pyspark
```

Select it in a target:

```yaml
targets:
  dev:
    engine:
      kind: spark
      mode: local
    catalog:
      kind: delta_path
      root: warehouse
```

- Relations are DataFrames. SQL models run with `spark.sql`, and inputs are bound by alias
  without session temp views.
- Sessions: `local`, `connect` (Spark Connect, including a local server), `active` (an existing
  session, as on Databricks) or `auto`. The engine stops only a session it built, and sets no
  Spark conf on a session it did not build or at run time.
- Catalogs: `delta_path` (Delta tables at `<root>/<schema>/<name>`, no metastore) and
  `spark_catalog` (tables and views by name through the session catalog).
- Reads `delta`, `parquet`, `csv`, `json` and `ndjson`. Cancel stops a unit's jobs through
  Spark job tags.
- This release reads and runs SQL; Delta write strategies arrive in a later release.
- pyspark is not a hard dependency: the `local` extra installs `pyspark[connect]` and
  `delta-spark`. Databricks runtimes and Databricks Connect bring their own pyspark.
- Needs Java 17 or 21 for local Spark. On Windows, run Spark in WSL.

Documentation: https://emanuel-luis.github.io/pipeloom/latest/plugins/spark/

License: Apache-2.0.
