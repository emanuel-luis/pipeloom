# Lineage with OpenLineage

The `openlineage` sink sends pipeloom runs to an [OpenLineage](https://openlineage.io) backend such as [Marquez](https://marquezproject.ai): one job per persisted model, its input and output datasets, and facets for the schema, row counts, data quality results and errors. Decided in [ADR-0046](../adr/0046-openlineage.md).

## Install

```console
$ pip install "pipeloom[openlineage]"
```

The extra installs `pipeloom-openlineage`, which brings `openlineage-python`. The core never imports either.

## Configure the sink

```yaml
events:
  sinks:
    - kind: openlineage
      transport:
        type: http
        url: http://localhost:15000
```

`transport` is an openlineage-python transport config: `type` plus that transport's keys (`http`, `kafka`, `file`, `console` and the others the client knows). Without `transport` the client reads `OPENLINEAGE_URL` or an `openlineage.yml` the way openlineage-python always does. A transport that needs credentials takes them as secret references, for example `api_key: ${secret:env/OPENLINEAGE_API_KEY}`; commands that run nothing (`validate`, `compile`) do not resolve them and send nothing.

| Option | Default | Meaning |
|---|---|---|
| `namespace` | `pipeloom.{target}` | namespace of the jobs; `{project}` and `{target}` are filled in |
| `dataset_namespace` | the job namespace | namespace of catalog relations; sources with a location follow the OpenLineage naming: `s3://bucket` and `raw/s` for `s3://bucket/raw/s`, `file` and the absolute path for local files, `dbfs` and the path for `dbfs:/...` |
| `transport` | none | an openlineage-python transport config |
| `inside_airflow` | `skip` | `skip` leaves Airflow tasks to the Airflow OpenLineage provider; `emit` reports them here too |

`failure_budget`, `buffered` and `buffer_size` work as for every sink.

## What it sends

- The run is the job `<project>`; each persisted model is the job `<project>.<model>`, whose runs carry a `parent` facet pointing at the pipeloom run.
- Run ids are uuid5 values derived from the project, the run id, the unit and the attempt, so every process of one run (unit tasks, `pipeloom finalize`, an Airflow task) reports the same ids. Each attempt of a retried unit is its own run.
- Inputs come from the manifest: the model's catalog relations and sources, minus the ephemeral models it inlines. The output is the model's relation.
- Facets: `jobType`, `sourceCodeLocation` (the model's file and line), `schema` (the written columns, else the contract), `outputStatistics` (rows and bytes written), `dataQualityAssertions` (the model's expectations) and `errorMessage` (the error code, message and traceback of a failed unit or run).

## Under Airflow

The Airflow OpenLineage provider already reports every task, with the DAG run as parent. To avoid two copies of every unit, the sink ignores the events of a unit that an Airflow task runs (`inside_airflow: skip`), and the pipeloom operators hand the provider the datasets and facets instead, through `get_openlineage_facets_on_start`, `..._on_complete` and `..._on_failure`:

| Execution mode | What the provider gets |
|---|---|
| LOCAL, VIRTUALENV | the datasets and facets built from the child's events, as the sink would build them |
| KUBERNETES | the static datasets of the manifest: the pod's events stay in the pod |
| WATCHER | each watcher reports the static datasets of its unit from the manifest; the producer's child carries the Airflow ids, so the sink stays silent |
| DATABRICKS | nothing from the operators: the sink in the Databricks task reports the units, since its events carry no Airflow ids |

Install `apache-airflow-providers-openlineage` and `pipeloom-openlineage` in the Airflow image; without either, the hooks return nothing. Set `inside_airflow: emit` only when the provider is not installed.

## Try it with Marquez

`examples/marquez/compose.yml` starts Marquez locally (API on port 15000, web UI on 13000), and `tools/marquez_check.py` runs the TPC-H polars example against it and checks the jobs, the lineage edges, the schema fields, the quality assertions and the parent run ids. From the repository root:

```console
$ uv run examples/tpch_data.py
$ docker compose -p pipeloom-marquez -f examples/marquez/compose.yml up -d
$ uv run --all-packages python tools/marquez_check.py
$ docker compose -p pipeloom-marquez -f examples/marquez/compose.yml down -v
```

Wait until `curl -fs http://localhost:15000/api/v1/namespaces` answers before the check. Each fact prints as `CHECK <name> ok` or `CHECK <name> FAIL <detail>`, and the tool exits 1 on any failure. `--keep` keeps the example's run artifacts. Then open http://localhost:13000 and pick the namespace `pipeloom.dev`.
