# pipeloom

Declarative, engine-agnostic data pipelines in YAML.

Pipelines are declared in YAML and compiled into an immutable plan. The same plan runs on pandas,
polars, PySpark or later engines, and runs locally, as one Databricks job, or as an Airflow DAG.
Every run produces a single event stream and artifacts you can query.

**Status: design phase.** Version 0.0.2 is a placeholder that reserves the package name. It
exposes `pipeloom --version` and nothing else. No functionality exists before 0.1.

![Context diagram: users, the YAML project, the CLI and API, engines, catalogs, orchestrators and event consumers.](docs/diagrams/01-context.svg)

## Goals

- Declarative YAML with layered validation and editor completion.
- An engine-agnostic core with one plugin per tool.
- End-to-end events and styled logs from one event stream.
- Persisted metadata for each run and each model.
- A Databricks renderer that produces one job per plan.
- A cosmos-style Airflow integration.
- Docs, CLI reference, JSON Schema and error catalog generated from code.
- Open-source hygiene from day one.

## Planned packages

Only `pipeloom` is published today. Plugin names are published with their first functional
release.

| Distribution | Role |
|---|---|
| `pipeloom` | Core: config, validation, plan, scheduler, events, CLI |
| `pipeloom-polars` | polars engine |
| `pipeloom-pandas` | pandas engine |
| `pipeloom-spark` | PySpark engine (Delta and Spark Connect) |
| `pipeloom-databricks` | Databricks job renderer and runner |
| `pipeloom-airflow` | Airflow DAG integration |
| `pipeloom-openlineage` | OpenLineage event sink |

## Documentation

- [Architecture](docs/architecture.md)
- [Architecture decision records](docs/adr/README.md)
- [Diagrams](docs/diagrams/)

## Roadmap

| Version | Scope |
|---|---|
| 0.0.2 | Reserve the name, skeleton, CI, release, governance, architecture and ADRs |
| 0.1 | Config models, loader, validation layers, graph, selectors, plan, manifest, event bus; CLI `validate`, `compile`, `ls`, `graph`, `schema`, `plugins` |
| 0.2 | Scheduler, executors, polars and pandas engines, basic write strategies, state store, contract suite |
| 0.3 | Incremental, merge, schema-change policy, expectations, retries, timeouts |
| 0.4 | Spark engine, run metrics, idempotent writes |
| 0.5 | Databricks renderer, deploy, run, repair |
| 0.6 | Airflow integration |
| 0.7 | OpenLineage, SCD2, deletes, `pipeloom retry` |
| 0.8 to 0.9 | Hardening, docs, API freeze candidates |
| 1.0 | Stable config, plugin API and artifact schemas |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Participation is governed by the
[Code of Conduct](CODE_OF_CONDUCT.md); security reports follow [SECURITY.md](SECURITY.md).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

## Author

Emanuel Luis ([@emanuel-luis](https://github.com/emanuel-luis)).
