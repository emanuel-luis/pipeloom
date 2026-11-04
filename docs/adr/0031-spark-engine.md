# ADR-0031: Spark engine

Architecture: [Section 3, Packages](../architecture.md)

Amends [ADR-0002](0002-engine-abstraction.md) and [ADR-0005](0005-packaging.md).

## Context

0.4 adds a Spark engine. Spark differs from polars and pandas in ways that shape the plugin: pyspark is large (a 450 MB source distribution) and is already present, in its own version, on Databricks runtimes and with Databricks Connect; a session is a heavyweight object shared by every worker thread; work happens when an action runs, which is at the write; and a session can be a local JVM, a Spark Connect client or a session someone else owns.

## Options

- **Depend on pyspark.** Simple, but installing a second pyspark breaks Databricks runtimes and Databricks Connect.
- **No hard dependency, extras for local use.** The plugin depends on pipeloom only; the extra `local` brings pyspark and Delta.
- **Job groups or tags for cancellation.** Job groups do not exist in Connect. Tags do: `SparkSession.addTag` and `interruptTag` in Connect, and `SparkContext.addJobTag` and `cancelJobsWithTag` in classic Spark. Classic `SparkSession.addTag` exists since 4.0 but did not tag the jobs of a DataFrame action in the 4.2 test run (`interruptTag` found nothing), so the engine uses the SparkContext calls there.
- **Session temp views or DataFrame binding for SQL aliases.** Temp views live in the session, so two units that use one alias collide. `spark.sql(query, **frames)` binds DataFrames without a name in the session.

## Decision

`pipeloom-spark` depends only on `pipeloom`. The extra `local` is `pyspark[connect]>=4.0.1,<4.3` and `delta-spark>=4.1,<4.5`, and `pipeloom[spark]` pulls `pipeloom-spark[local]`. The floor is pyspark 4.0.1; the upper bounds follow the newest Delta release that supports the Spark version. Connect-only clients may install `pyspark-client` instead.

The spec module imports no Spark library. The engine and catalog modules also import pyspark only inside functions, so a Databricks environment without extras loads them.

Sessions have four modes: `local`, `connect`, `active` and `auto`. Only a session the engine built is stopped. The engine sets no Spark conf on a session it did not build and none at run time on any session; `conf` applies when the engine builds the session, and `conf` with `active` is an error. Per-write settings are DataFrameWriter options, which take precedence over session conf.

SQL aliases are common table expressions over `{keyword}` fields that `spark.sql` binds to DataFrames, with the braces of the user's query doubled. A query with no aliases is passed unchanged, since Spark does not format a query that has no keywords.

Each unit adds one job tag for the length of its reads, body and write (a session tag on Connect, a job tag on classic), and `cancel` interrupts the jobs with that tag. An interrupt sent before the unit's first job exists finds nothing, so a short-lived thread repeats it until the unit's scope ends (at most a minute). `persist` registers the persisted frame with the current unit, which unpersists it when its scope exits; a memoized ephemeral persisted by its first consumer is recomputed by later consumers once that unit ends, which is correct and slower.

Two catalogs ship: `delta_path` (tables at `<root>/<schema>/<name>`, no metastore) and `spark_catalog` (names through the session catalog, with views). The engine binds its session to the catalog for the run.

Spark tests opt in with the test-only variable `PIPELOOM_TEST_SPARK=classic|connect`, and skip when pyspark, `java` or a non-Windows host is missing. Classic and Connect run as two pytest invocations because they cannot share a process. CI has a `spark` job on Ubuntu with Java 17; it is not a required check until it has two green runs on main. The `typecheck` job installs the `spark` dependency group so the plugin is checked against the real pyspark types.

This release declares only what it implements: reads, SQL, cancel and the `table` and `ephemeral` materializations, with no write strategy. `EngineContract` write tests that did not name the `overwrite` strategy (`test_metrics`, `test_two_threads_materialize_at_once`) now require it, as the other write tests do.

## Consequences

- Databricks and Databricks Connect users install `pipeloom-spark` without extras and keep their own pyspark.
- A Spark session is never reconfigured behind the user's back; the cost is that conf cannot be changed per run.
- Cancel is cooperative: it stops jobs, not Python code between jobs.
- The first CI run downloads pyspark and, for Delta, jars from Maven; a Maven outage fails the job.
- Delta writes, idempotency, quality and the state store build on this session and catalog layer in later releases.

## Status

Accepted.

## Date

2026-11-04
