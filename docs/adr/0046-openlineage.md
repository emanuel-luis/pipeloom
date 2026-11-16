# ADR-0046: OpenLineage

## Context

The roadmap (section 3, End-to-end observability) asks for a `pipeloom-openlineage` sink in 0.9: unit events become OpenLineage run events with the job `<project>.<model>` and a parent run facet for the pipeloom run, datasets come from the manifest, the facets cover schema, output statistics, data quality assertions and errors, the transport is the openlineage-python client, and Marquez is the reference backend. Under Airflow the operators implement `get_openlineage_facets_on_complete`, so the Airflow OpenLineage provider reports the tasks with the DAG as parent, and the sink must stay silent there or every unit is reported twice (roadmap risk "Duplicate OpenLineage events under Airflow").

A sink only sees events ([ADR-0010](0010-events-sinks-artifacts.md)). Events name units and relations but not the plan: the inputs of a unit, its source locations, its contract and its file are in the manifest. One pipeloom run can span many processes: a `pipeloom run`, process-boundary children whose events the parent forwards, one unit invocation per orchestrator task, and `pipeloom finalize` ([ADR-0034](0034-core-orchestration-hooks.md)). Each process builds its own sinks. The plugin API is (0, 5) and not yet released ([ADR-0047](0047-run-history-and-webhook.md)), so additive changes stay in (0, 5).

Models are moving from YAML files with SQL or Python bodies to `@model`-decorated Python functions (ADR-0050, in progress). SQL bodies go away before 0.9.0, and node fields may be renamed.

## Options

- **Read the manifest from disk in the sink.** The sink would guess the artifacts root and the run folder, and a unit invocation reads the manifest from `--manifest`, which can sit anywhere.
- **Put datasets on the events.** Every unit event grows lineage fields that only one sink reads.
- **A sink hook that hands over the plan.** Chosen: `plan_ready(PlanInfo)`, optional, called once per invocation that has a plan.
- **Schema from the contract only.** Most models declare no contract.
- **Schema from what was written.** Chosen, through an additive `model.written.columns` field; the contract fills in when the write path does not know the columns.
- **Random run ids per process.** The finalize process and the unit tasks of one run would report unrelated OpenLineage runs.
- **uuid5 run ids derived from the pipeloom ids.** Chosen: every process derives the same ids without talking to the others.
- **The `sql` job facet.** SQL bodies go away with the Python-first pivot; the facet would describe a body pipeloom no longer has.
- **The `sourceCodeLocation` job facet.** Chosen: the model's file and line, which both authoring styles have.
- **Inside Airflow, the sink and the provider both emit.** Duplicate jobs and runs in the backend.
- **Inside Airflow, only the provider emits.** Chosen as the default (`inside_airflow: skip`), with `emit` for setups without the provider.

## Decision

**Core hook (additive in plugin API (0, 5)).** `pipeloom.events.bus.PlanInfo(manifest, project_dir)` holds the run's manifest as `manifest_dict(plan)` returns it (`pipeloom.manifest/1`) and the absolute project dir. `EventBus.plan_ready(info)` calls `plan_ready(info)` on every enabled sink that defines it; sinks without it are skipped, and a failure counts against the sink's failure budget like a failed delivery. `BufferedSink` queues the plan in order with the events and never drops it. `pipeloom run` calls the hook right after `run.started` (a unit invocation, which emits no `run.started`, calls it at the same point), and `pipeloom finalize` calls it before `run.completed`. A process-boundary child reports to its parent only, so the parent's sinks get the plan once.

**`model.written.columns` (additive in `pipeloom.event/1`).** The table's columns after the write, as the schema policy resolved them (`name`, `dtype`, `nullable`; scd2 tables include their validity columns). Null on the intermediate path, for views and for replayed writes.

**Distribution.** `pipeloom-openlineage` (module `pipeloom_openlineage`, sink kind `openlineage`, root extra `openlineage`) depends on `openlineage-python>=1.52.0,<2`. The floor is what `apache-airflow-providers-openlineage` requires, so the sink and the provider can share one client inside an Airflow image. The spec imports no OpenLineage code; the client is built at the first event that needs it, from `transport` when given (an openlineage-python transport config), else from `OPENLINEAGE_URL` or `openlineage.yml` as the client reads them. A transport that still holds a `${secret:...}` reference (a command that never resolves secrets) sends nothing.

**Options.** `namespace` (default `pipeloom.{target}`; `{project}` and `{target}` are filled in), `dataset_namespace` (default: the job namespace), `transport`, and `inside_airflow: skip|emit` (default `skip`).

**Jobs and runs.** The pipeloom run is the job `<project>`; each persisted model is the job `<project>.<model>`. Run ids are uuid5 values in a fixed pipeloom namespace: `run:{project}:{run_id}` for the run and `unit:{project}:{run_id}:{unit_id}:{attempt}` for one attempt of a unit; the project keeps two projects that pass the same `--run-id` apart. `pipeloom_openlineage.sink.run_uuid` and `unit_uuid` are the only place that computes them, so the Airflow operators use the same functions. Every unit run carries the `parent` facet (and its `root`) pointing at the pipeloom run.

**Event mapping.** `run.started` is START of the run job. `unit.started` is START of the unit run; `unit.succeeded` is COMPLETE; `unit.failed` and `unit.timed_out` are FAIL with the `errorMessage` facet (the code and message, and the full traceback as `stackTrace`); `unit.cancelled` is ABORT, and a unit cancelled or skipped before it started has no OpenLineage run. Each attempt of a retried unit is its own run. `run.completed` is COMPLETE, FAIL or ABORT by its status, with an `errorMessage` facet listing the failed units; a `run.completed` with `replay: true` is skipped, since the first finalize reported it. A run driven by an orchestrator gets no START for the run job: its unit tasks report their runs with the parent facet, and finalize reports the end.

**Datasets.** Inputs of a unit are the inputs of all its nodes, minus the ephemeral models it inlines. A model input is its catalog relation (`catalog.schema.name`) in `dataset_namespace`. A source with a `table` is that table in `dataset_namespace`; a source with a `location` follows the [OpenLineage naming spec](https://openlineage.io/docs/spec/naming): a URI with a host (`s3://bucket/raw/s`) has `scheme://host` as namespace and the path without its leading slash as name (`raw/s`); a `file:` URI has namespace `file` and the absolute path as name; any other scheme without a host (`dbfs:/Volumes/c/s/v/x`) has the scheme as namespace and the path as reported; a plain relative path is joined to the project directory under namespace `file`. The output is the persisted model's relation.

**Facets.** `jobType` (`BATCH`, integration `PIPELOOM`, `JOB` for the run, `MODEL` for a unit); `sourceCodeLocation` (type `file`, `path` the node's file relative to the project dir, `url` the same with `#L<line>` when the manifest has the line); `schema` on the output, from `model.written.columns`, else from the contract; `outputStatistics` on a completed output (`rowCount` and `size` from `unit.succeeded`); `dataQualityAssertions` from the unit's `model.quality_evaluated` results (`assertion` is the expectation kind, `success` means no failed rows, `severity` is the action). The spec defines `dataQualityAssertions` as an input dataset facet. pipeloom checks the rows it writes, so the sink puts it in the output dataset's facets, where Marquez shows it on the dataset; the sink casts the type. No `sql` facet.

**Manifest access.** The sink reads only `metadata.project`, `metadata.target`, `units`, `nodes[].file`, `line`, `inputs`, `relation` and `columns`, and `sources[].location` and `table`, each through one small accessor function, so a rename of node fields in the Python-first pivot touches one place.

**No duplicates under Airflow.** Events of a unit run by an Airflow task carry `orchestrator_id.airflow_dag_id` in their context. With `inside_airflow: skip` the sink ignores every such event, the run events included, and the provider reports the task. The Airflow operators implement `get_openlineage_facets_on_complete` through a mixin, by execution mode: LOCAL and VIRTUALENV capture the child's events and build the same inputs, outputs and facets as the sink; KUBERNETES returns the static datasets from the manifest; DATABRICKS relies on the sink in the Databricks task (its events carry no Airflow ids). A test runs one task with the sink configured and asserts one set of events.

**Reference check.** `examples/marquez/compose.yml` (compose project `pipeloom-marquez`: API on 15000, admin on 15001, web on 13000, Postgres without a host port) and `uv run --all-packages python tools/marquez_check.py [--keep]` run a project against Marquez and check five things: the jobs, the lineage edges against the manifest, the schema fields, the quality assertions and the parent run ids. It needs Docker and is run by hand before a release, not in CI.

**Release.** `release.yml` gets `publish-pipeloom-openlineage` (environment `pypi-openlineage`), joins the `known` list and the outputs loop, and `github-release` waits for it. CI type-checks and tests the plugin like the others.

**Codes.** None. PLX116 stays unused.

## Consequences

Lineage works for `pipeloom run`, process-boundary runs and runs an orchestrator drives task by task, with stable ids across processes and retries of finalize. Sinks that want the plan get it without reading files, and third-party sinks keep working unchanged. The `columns` field and the hook are additive, so the plugin API stays (0, 5). Column-level lineage, dataset version facets and an input statistics facet are left for later; the `sql` facet is not planned. The Airflow mixin, the Marquez compose file and its check, and the docs guide land with the second half of this item.

## Status

Accepted.

## Date

2026-11-15
