# pipeloom

Declarative, engine-agnostic data pipelines in YAML.

pipeloom 0.2 reads a YAML project, checks it in layers, builds the dependency graph, writes the
execution plan as a manifest and runs it on polars or pandas.

- [Quickstart](quickstart.md)
- [Running a pipeline](guide/running.md)
- Reference: [command line](reference/cli.md), [JSON Schema](reference/schema.md), [run artifacts](reference/artifacts.md), [error codes](reference/errors.md)
- [Architecture](architecture.md)
- [Architecture decision records](adr/README.md)
