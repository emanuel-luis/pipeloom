# pipeloom

Declarative, engine-agnostic data pipelines in YAML.

pipeloom 0.3 reads a YAML project, checks it in layers, builds the dependency graph, writes the
execution plan as a manifest and runs it on polars or pandas, with incremental models, merge and
partition writes, data-quality expectations, retries, timeouts and cancellation.

- [Quickstart](quickstart.md)
- Guides: [running a pipeline](guide/running.md), [incremental models](guide/incremental.md),
  [write strategies](guide/write-strategies.md), [data quality](guide/quality.md),
  [retries and cancellation](guide/retries-and-cancellation.md)
- Reference: [command line](reference/cli.md), [JSON Schema](reference/schema.md), [run artifacts](reference/artifacts.md), [error codes](reference/errors.md)
- [Architecture](architecture.md)
- [Architecture decision records](adr/README.md)
