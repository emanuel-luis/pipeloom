# pipeloom

Engine-agnostic data pipelines: Python models marked with @model, YAML for the project.

pipeloom discovers the `@model` functions of a project, checks it in layers, builds the
dependency graph, writes the execution plan as a manifest and runs it on polars, pandas or Spark,
with incremental models, merge, partition and scd2 writes, contracts, data-quality expectations,
unit and data tests, retries, timeouts and cancellation.

- [Quickstart](quickstart.md)
- Guides: [running a pipeline](guide/running.md), [incremental models](guide/incremental.md),
  [write strategies](guide/write-strategies.md), [data quality](guide/quality.md),
  [retries and cancellation](guide/retries-and-cancellation.md)
- Reference: [command line](reference/cli.md), [JSON Schema](reference/schema.md), [run artifacts](reference/artifacts.md), [error codes](reference/errors.md)
- [Architecture](architecture.md)
- [Architecture decision records](adr/README.md)

<figure markdown="span">
  ![Who uses pipeloom and which example to start from: an analytics engineer builds a laptop lakehouse (tpch_polars) or runs the same code on any engine (the tpch projects); a platform engineer deploys to an orchestrator (tpch_databricks, examples/airflow) or tracks lineage (examples/marquez); a plugin author adds an engine and checks it with the conformance kit.](diagrams/use-cases.light.svg#only-light)
  ![Who uses pipeloom and which example to start from: an analytics engineer builds a laptop lakehouse (tpch_polars) or runs the same code on any engine (the tpch projects); a platform engineer deploys to an orchestrator (tpch_databricks, examples/airflow) or tracks lineage (examples/marquez); a plugin author adds an engine and checks it with the conformance kit.](diagrams/use-cases.dark.svg#only-dark)
  <figcaption>Where to start, by role. Source: <a href="https://github.com/emanuel-luis/pipeloom/blob/main/docs/diagrams/use-cases.d2">use-cases.d2</a>.</figcaption>
</figure>
