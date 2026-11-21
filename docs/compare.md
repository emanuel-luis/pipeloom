# Compared with other tools

pipeloom overlaps with several well-known tools. This page says where it differs and when another tool is the better
choice. The [market landscape](landscape.md) has the research and sources behind it.

## In one table

| Tool | Models are written in | Where they run | Scheduling | A good fit when |
|---|---|---|---|---|
| pipeloom | Python functions, SQL through `ctx.sql` | DuckDB, polars, pandas, Spark, locally or rendered to Databricks and Airflow | None: it renders to orchestrators | The same Python pipeline must run on a laptop and on a cluster, with typed artifacts other tools can read |
| dbt | SQL with Jinja, Python models on some warehouses | Inside the warehouse, through adapters | None in dbt Core | A SQL-first analytics team works on one warehouse and wants the largest ecosystem |
| SQLMesh | SQL and Python | Warehouses and engines, through adapters | A built-in scheduler for model intervals | Change categorization and virtual development environments matter most |
| Hamilton | Python functions as dataflow nodes | In process, on dataframe libraries | None | Python transformations and feature engineering, often inside other applications |
| Kedro | Python nodes in pipelines, with a data catalog | In process, deployed through plugins | None | Data science and ML projects that want a project template and a catalog |
| Dagster | Python assets and ops | Its own runtime | Schedules, sensors and a UI | One product should orchestrate, observe and catalog data assets |
| Spark Declarative Pipelines | Python and SQL declarations | Spark, and Databricks as Lakeflow | Managed by the platform | Everything already lives on Spark or Databricks |

## What pipeloom does differently

- **One model, any engine.** A model is a Python function. SQL runs through `ctx.sql` and is translated to the
  target's dialect, and [portable models](guide/portable-models.md) use one frame API, so the same code runs on DuckDB
  in a test and on Spark in production. The [conformance kit](plugins/writing-an-engine.md) holds every engine to the
  same behavior.
- **No templating layer.** Models are plain Python, configuration is YAML with a JSON Schema, and there is no Jinja.
  Editors, linters and type checkers work on models as on any other Python code.
- **Artifacts are the contract.** The manifest, `run_results.json`, the events and the attempt files have published
  JSON Schemas under a stability policy. Orchestrators, lineage tools and the docs site read files, not internals.
- **Export, not a server.** pipeloom renders a plan for Databricks or Airflow and leaves scheduling, retries of whole
  jobs and any UI that starts runs to them. There is no service to run.
- **Data quality in the write path.** Expectations judge each batch before the write and can quarantine rows,
  contracts check columns, and data tests judge the table after the write, all with diagnostics that have codes.

## When to choose something else

- Your team writes SQL only and works on one warehouse: dbt or SQLMesh fit that workflow more directly.
- You need zero-copy development environments and promotion: SQLMesh's virtual environments do that, and pipeloom
  deliberately does not (it offers per-user naming and `--defer`).
- You want scheduling, sensors and a UI that starts runs: use an orchestrator such as Airflow or Dagster, and run
  pipeloom inside it.
- You stream data continuously: pipeloom is batch only.

## Using pipeloom with them

pipeloom is designed to sit next to these tools rather than replace them:

- **Airflow** runs a pipeloom project as a DAG with one task or one watcher per model ([Airflow plugin](plugins/airflow.md)).
- **Databricks** runs it as one job per pipeline ([Databricks plugin](plugins/databricks.md)).
- **Dagster** support is planned as a plugin in 1.4 ([roadmap](roadmap.md)).
- **OpenLineage** consumers such as Marquez receive its lineage ([OpenLineage guide](guide/openlineage.md)).
- Any other orchestrator can follow the [orchestrator protocol](guide/orchestrators.md).
