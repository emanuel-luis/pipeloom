# Spark engine

`pipeloom-spark` runs models on [Apache Spark](https://spark.apache.org) DataFrames and Spark SQL
and writes [Delta Lake](https://delta.io) tables. It works on a local Spark, through Spark Connect,
and inside a session someone else owns, such as a Databricks runtime. It also provides the `table`
state store, which keeps run state in a Delta table.

```console
$ pip install "pipeloom[spark]"      # local Spark: pulls pipeloom-spark[local]
$ pip install pipeloom-spark         # Databricks, Databricks Connect, or any environment with pyspark
```

```yaml
targets:
  dev:
    engine: {kind: spark, mode: local, conf: {spark.sql.shuffle.partitions: "4"}}
    catalog: {kind: delta_path, root: warehouse}
    state: {kind: table, path: warehouse/_state}
```

## Installation

pyspark is not a hard dependency. Databricks runtimes and Databricks Connect ship their own
pyspark, and installing another one breaks them, so `pipeloom-spark` depends only on `pipeloom`.

| Where | Install |
| --- | --- |
| Local Spark (a laptop, CI) | `pipeloom-spark[local]`, or `pipeloom[spark]`. The extra installs `pyspark[connect]` (4.0.1 up to, not including, 4.3) and `delta-spark` (4.1 up to, not including, 4.5). |
| A Databricks runtime or serverless | `pipeloom-spark` with no extras. |
| Databricks Connect | `pipeloom-spark` with no extras, next to `databricks-connect`. |
| A thin Spark Connect client | `pipeloom-spark` and `pyspark-client`. Never together with `pyspark`: both own the `pyspark` package. |

Local Spark needs Java 17 or 21 on the path. Spark 4 runs with ANSI mode on by default, so an
invalid cast or an overflow fails the query instead of giving null. Check SQL models that relied on
the old behavior.

On Windows, run Spark in WSL. Windows Python workers do not work without winutils and die with a
connection reset. Install Java and `uv` in the WSL distribution and run the project there.

## Session modes

| Mode | Behavior |
| --- | --- |
| `local` | Builds a session with `master` (default `local[*]`), `app_name` and `conf`, and adds the Delta jars, SQL extension and catalog unless `delta: false`. |
| `connect` | Builds a Spark Connect session from `remote` or `SPARK_REMOTE`. A URL that starts with `local` starts a local Connect server with the same conf. |
| `active` | Uses the existing session and never builds, configures or stops anything. |
| `auto` (default) | `active` when `DATABRICKS_RUNTIME_VERSION` is set or a session exists, else `connect` when `remote` or `SPARK_REMOTE` is set, else `local`. |

Only a session the engine built is stopped. The engine sets no Spark conf on a session it did not
build, and sets none at run time on any session: `conf` applies when the session is built, and
`conf` with mode `active` is an error. Building a session when one already exists in the process
fails and tells you to use mode `active`. Classic and Connect cannot share one Python process.

`boundary: task` with local Spark starts one JVM per unit. It works, and it is slow.

## Catalogs

- `delta_path`: Delta tables at `<root>/<schema>/<name>`; the target's catalog name is ignored. No
  metastore, and it works across processes. `root` may be a URI such as `s3://` or `abfss://`. It
  cannot hold views.
- `spark_catalog`: tables and views by name through the session catalog (`catalog` option, else
  the current catalog). It is the base the Unity Catalog support of a later release reuses. A view
  is created with `CREATE OR REPLACE VIEW` over a table, `delta` or `parquet` input.

## Capabilities

| Capability | Value |
| --- | --- |
| Source formats | `delta`, `parquet`, `csv`, `json`, `ndjson` |
| Write strategies | `append`, `overwrite`, `merge`, `replace_partitions` |
| Materializations | `table`, `incremental`, `view`, `ephemeral`, `intermediate` |
| Expectations and quarantine | yes |
| SQL models and views | yes, inputs bound by alias; views on `spark_catalog` |
| Idempotent writes | yes, by token in the Delta commit |
| Metrics | rows and bytes, from the commit history |
| Cancel | yes, through Spark job tags |
| Portable frames (`frame_api: narwhals`) | yes |

`csv` reads a header and infers types; `json` is one multi-line document and `ndjson` is one
document per line. Reader options pass through as strings.

## Write strategies

Every write is a DataFrameWriter option or a SQL statement. No Spark conf is touched.

| Strategy | What the engine does |
| --- | --- |
| `append` | Appends. New columns are added in the same write (`mergeSchema`). |
| `overwrite` | Overwrites data and schema (`overwriteSchema`), so any result of the schema policy is accepted. |
| `replace_partitions` | Overwrites with a `replaceWhere` predicate built from the distinct `partition_by` tuples of the batch. It works on partitioned and unpartitioned tables. More than 10,000 distinct tuples fails and recommends `merge`. An empty batch replaces nothing and writes only the token commit. |
| `merge` | A SQL `MERGE INTO` on `unique_key`, with `update_condition` and hard or soft `deletes`. Rows with a null key never match and are inserted. A key repeated in the batch is PLM206. A merge into a table that does not exist yet is the first load, written as an append. |

A full refresh and a first build follow the core rules described in the
[write strategies guide](../guide/write-strategies.md).

### Schema changes

The core decides the final columns from `on_schema_change`. The engine writes added columns in
the write itself. When columns were removed or a type was widened, the engine first runs one migration
commit: it reads the table, aligns it to the final columns and overwrites it with
`overwriteSchema`, keeping the table's partitioning. The migration commit carries no transaction
options and no token, so it never uses up the write version and is not counted in the metrics.

### Tables created by `spark_catalog`

A table that `saveAsTable` creates does not keep the writer's commit metadata. When the target does
not exist, the catalog creates it empty first, without a token, and the tokened write is an
ordinary append into it.

## Idempotent writes

Each write has the token `<run_id>:<model>`, which the engine records in Delta.

- The transaction app id is `pipeloom/<project>/<model>`. Quarantine and intermediate writes add
  the suffix `#quarantine` or `#intermediate`.
- The transaction version is the run start in epoch milliseconds. It comes from the run start time
  the core passes with the write (see `--run-started-at` in [Running a
  pipeline](../guide/running.md)), never from the session.
- The commit's `userMetadata` holds `{"pipeloom": {"v": 1, "token", "commit_meta", "result"}}`.

Before a write, the engine scans the last 100 commits (`DESCRIBE HISTORY`) for the token. A hit
returns the stored result and writes nothing, so rerunning a finished run id changes no data.

For a merge, Delta applies the transaction options to a MERGE only through session conf, which
would leak into other units of the same session. So the MERGE runs as plain SQL and is followed by
an empty append that carries the token and the stored result. Running the same MERGE again with the
same batch converges on the same table, so a crash between the two commits is safe, and the token
commit is the guard against two writers.

### PLM207 and its limits

The transaction version only grows. When Delta skips a write and no commit carries the token, the
engine looks again (a racing attempt may have won), then through the full history. If the token is
nowhere, a later version already exists for this model and the write fails with PLM207. Nothing was
written by append, overwrite or replace_partitions. The causes:

- a newer run of the same model finished first;
- an overlapping run started with `--no-lock`;
- a host clock that is behind, since the version comes from a timestamp. Clock skew between
  machines that write the same model can refuse a legitimate write.

For a merge the guard can only refuse the token commit, so the MERGE itself has already been
applied when PLM207 is raised.

PLM207 is permanent: `retry_on: transient` does not repeat it. To turn the guard off, set
`txn_guard: false` on the engine. Then no transaction options are sent and only the history check
applies, which still stops a repeated token, but nothing stops an older run from writing after a
newer one.

## Metrics

Rows and bytes come from the table's commit history, so there is no extra Spark job. Before the
write the engine notes the latest version. After it, the engine sums `operationMetrics` over the
commits after that version whose token is this write's, plus the newest MERGE commit for a merge.
Fields a commit does not report stay null in `run_results.json`.

A concurrent MERGE on the same table inside that window can be mistaken for ours in the metrics
(the data is not affected).

## Expectations and quarantine

The engine declares the `quality` capability. All counts and aggregates of a model's checks are
evaluated in one Spark job. Rows dropped by a check go to `<model>__quarantine` with the usual
`_pipeloom_run_id`, `_pipeloom_failed` and `_pipeloom_ts` columns, under the `#quarantine` app id.
`regex` is a Java regular expression applied unanchored.

## The table state store

```yaml
state: {kind: table, table: ops._pipeloom_state}   # by catalog name
# state: {kind: table, path: warehouse/_state}     # or by path; set exactly one of the two
```

`table` keeps watermarks, runs, units and the run lock in one Delta table, partitioned by `kind`.
It is shared, so it works with `boundary: task` (PLC503 passes). Compare-and-set and the lock lease
are single `MERGE` statements, and Delta conflicts are retried up to 10 times. The store reuses the
engine's session; its `session` option builds one only when none exists, and only a session it
built is stopped. Each state call is a Spark job, so it is slower than sqlite or json. The table
grows with every run, and Delta's `OPTIMIZE` and `VACUUM` apply to it. Lease times use the client
clock.

## Intermediates

`materialized: intermediate` writes the model's result as a Delta table under
`<artifacts.root>/runs/<run_id>/intermediate/<model>`, under the `#intermediate` app id. The
consumer reads it back as a Delta path. With a remote Spark Connect server, the client and the
server must both see `artifacts.root`, so set it to a shared location such as a Volume or an
object-store URI. See [Running a pipeline](../guide/running.md) for the TTL.

## SQL

A SQL model's inputs are bound without session temp views: each alias becomes a common table
expression over a DataFrame passed to `spark.sql`, so two units can use the same alias at the
same time and an alias shadows a catalog table of the same name.

## Cancellation

Each unit adds a job tag (`pipeloom:<run id>:<unit id>`) for the length of its reads, body, checks
and write. `cancel` interrupts the jobs with that tag (`SparkContext` job tags on classic Spark,
session tags on Connect) and keeps interrupting, for up to a minute, until the unit ends.
Cancellation is cooperative: Python code between jobs is not stopped, and a Delta commit either
completes or does not happen.

## Timestamps

The engine reads timestamps as microseconds since the epoch, so the Python process time zone
never changes a value. User code that calls `collect()` on a classic session gets naive local
datetimes for timestamp columns, converted in the time zone of the Python process. Convert in
Spark (for example with `unix_micros`) when the exact instant matters.

## Running the tests

Spark tests opt in with `PIPELOOM_TEST_SPARK=classic` or `connect` and skip otherwise (pyspark,
`java` on the path and a non-Windows host are needed). Classic and Connect run as two pytest
invocations. A test that starts a second JVM also needs `PIPELOOM_TEST_SPARK_HEAVY=1`.
