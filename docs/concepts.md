# How pipeloom thinks

This page names the moving parts and how they connect. The [glossary](architecture.md#2-glossary) has the short
definitions; the [architecture](architecture.md) has the full design.

## A project

A project is a folder with a `pipeloom.yml`. It holds:

- **Models**: Python functions marked with `@model`, anywhere under the model paths. A model's name is the function
  name, and its parameters are its inputs: a parameter binds the model of that name, or a source table of that name.
- **Sources**: tables that exist outside the project, declared in groups under `sources:` in `pipeloom.yml`.
- **Properties files**: any other YAML file under the model paths. They describe and configure models by name
  (descriptions, columns, tests) and never declare a model.
- **Folder config**: the `models:` tree of `pipeloom.yml`, whose `+key` settings apply to every model in a folder.

When the same setting comes from several places, the most specific wins: the decorator, then the properties file,
then the folder, then the project.

## Targets, engines and catalogs

A **target** is a named environment: which **engine** runs the models (DuckDB, polars, pandas, Spark), which
**catalog** stores the tables (a DuckDB file, folders of Parquet or Delta, Unity Catalog), how names are built, and
where run **state** (watermarks, run history) is kept. The same project runs on several targets, for example `dev` on
DuckDB and `prod` on Databricks.

A model's **materialization** says what it becomes (`table`, `incremental`, `view`, `ephemeral`, `intermediate`), and
its **write strategy** says how rows land (`append`, `overwrite`, `replace_partitions`, `merge`, `scd2`). See
[write strategies](guide/write-strategies.md).

## From project to plan

pipeloom never runs your files directly. It goes through fixed stages:

1. **Discover** the `@model` functions. Decorators with literal arguments are read with `ast`, without importing
   your code.
2. **Validate** in layers: YAML, schema, references, graph, plan semantics, engine capabilities and Python
   callables. Each problem is a diagnostic with a code, a file and a line.
3. **Compile** the dependency graph into an immutable **plan**. The plan is split into **units**: one persisted model
   plus any ephemeral models inlined into it. A unit is what a scheduler or an orchestrator runs.
4. **Write the manifest**, `manifest.json`: the plan as a document, with a **plan hash** that ties every run to the
   exact plan it ran. Per-model checksums let `state:modified` select only what changed.

## Running

A **run** executes a plan, or a selection of it, under one `run_id`. A ready-queue scheduler starts each unit as soon
as its inputs are done; each try of a unit is an **attempt**, retried and timed out as configured. Expectations judge
each batch before the write, contracts check the columns, and data tests judge the table after it.

Every run writes the same **artifacts** under `target/runs/<run_id>/`: the manifest, `run_results.json`, the events and
one attempt file per unit. They have published [JSON Schemas](reference/artifacts.md), so other tools can read them
without importing pipeloom.

## Events

Everything that happens is a typed **event** on one bus: diagnostics, unit starts and ends, rows written, quality
results. **Sinks** subscribe to it: the console, JSON logs, a webhook for alerts, OpenLineage. Logs, metrics and lineage
come from the same stream, so they always agree.

## Orchestrators

pipeloom does not schedule. It **renders** a plan for an orchestrator, which then runs one unit per task with the same
command and the same `run_id`, and finishes with `pipeloom finalize`. The Databricks and Airflow plugins do this for
you; for anything else, the [orchestrator protocol](guide/orchestrators.md) describes the contract.

## Plugins

Engines, catalogs, state stores, sinks and orchestrator renderers are plugins found through Python entry points. The
core never imports a plugin until a config names it, and every engine passes the same
[conformance kit](plugins/writing-an-engine.md).

<figure markdown="span">
  ![From the author's YAML and @model files to pipeloom core, which runs engines that write tables and state, writes run artifacts, sends events to sinks such as OpenLineage, and renders plans for Databricks and Airflow.](diagrams/context.light.svg#only-light)
  ![From the author's YAML and @model files to pipeloom core, which runs engines that write tables and state, writes run artifacts, sends events to sinks such as OpenLineage, and renders plans for Databricks and Airflow.](diagrams/context.dark.svg#only-dark)
  <figcaption>The pieces around pipeloom core. Source: <a href="https://github.com/emanuel-luis/pipeloom/blob/main/docs/diagrams/context.d2">context.d2</a>.</figcaption>
</figure>
