# pipeloom

**Data pipelines as Python functions, run on any engine and any orchestrator.**

A model is a Python function marked with `@model`. Its parameters are its inputs, and its body returns a frame or
runs SQL. pipeloom checks the project, compiles it into a plan, and runs that plan on DuckDB, polars, pandas or Spark,
on your laptop, as a Databricks job or as an Airflow DAG. Every run writes the same events and artifacts.

```console
$ uvx --from "pipeloom[duckdb]" pipeloom demo
```

That one command writes a sample project, builds it with its tests and writes its docs site. The
[Quickstart](quickstart.md) explains each step.

```python
from pipeloom import Expectation, ModelContext, model, row_count


@model(expectations=[Expectation(name="not_empty", check=row_count(min=1), on_violation="fail")])
def revenue_by_region(ctx: ModelContext, orders_enriched, lineitem_revenue):
    return ctx.sql("""
        select o.region, round(sum(l.net_revenue), 2) as net_revenue
        from orders_enriched o
        join lineitem_revenue l on o.order_key = l.order_key
        group by o.region
    """)
```

## What you get

<div class="grid cards" markdown>

-   **One project, many engines**

    ---

    The same models run on DuckDB, polars, pandas and Spark. SQL in `ctx.sql` is translated to each engine's
    dialect, and [portable models](guide/portable-models.md) use one frame API everywhere.

-   **Checked before it runs**

    ---

    Layered validation with file and line diagnostics, contracts, unit tests with given and expected rows, and data
    tests after the write. Every diagnostic has a code you can [look up](reference/errors.md).

-   **Incremental and safe writes**

    ---

    Watermarks with a lookback, `merge` with deletes, `replace_partitions`, `scd2`, schema-change policies,
    quarantine, retries, timeouts and idempotent writes.

-   **Runs where your data lives**

    ---

    Locally, as one Databricks job per pipeline, or as an Airflow DAG with a task or a watcher per model. Any other
    orchestrator follows the [orchestrator protocol](guide/orchestrators.md).

-   **Observable by default**

    ---

    One typed event stream feeds the console, JSON logs, webhooks and [OpenLineage](guide/openlineage.md). Every run
    writes a manifest and `run_results.json` with published JSON Schemas.

-   **Stable contracts**

    ---

    Since 1.0 the CLI, the config, the plugin API and the artifact schemas follow
    [Semantic Versioning](reference/stability.md).

</div>

## Where to go next

| If you want to | Read |
|---|---|
| Run something in a minute | [Quickstart](quickstart.md) |
| See a real use case end to end | [Examples](examples/index.md) |
| Understand the moving parts | [Concepts](concepts.md) |
| Decide whether pipeloom fits | [Compared with other tools](compare.md) |
| Fix an error | [Error codes](reference/errors.md) and [Troubleshooting](faq.md) |

<figure markdown="span">
  ![Who uses pipeloom and which example to start from: an analytics engineer builds a laptop lakehouse (tpch_polars) or runs the same code on any engine (the tpch projects); a platform engineer deploys to an orchestrator (tpch_databricks, examples/airflow) or tracks lineage (examples/marquez); a plugin author adds an engine and checks it with the conformance kit.](diagrams/use-cases.light.svg#only-light)
  ![Who uses pipeloom and which example to start from: an analytics engineer builds a laptop lakehouse (tpch_polars) or runs the same code on any engine (the tpch projects); a platform engineer deploys to an orchestrator (tpch_databricks, examples/airflow) or tracks lineage (examples/marquez); a plugin author adds an engine and checks it with the conformance kit.](diagrams/use-cases.dark.svg#only-dark)
  <figcaption>Where to start, by role. Source: <a href="https://github.com/emanuel-luis/pipeloom/blob/main/docs/diagrams/use-cases.d2">use-cases.d2</a>.</figcaption>
</figure>
