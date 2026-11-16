# pipeloom

Engine-agnostic data pipelines: Python models marked with @model, YAML for the project.

pipeloom discovers the `@model` functions of a project, validates it in layers, builds the
dependency graph, compiles the execution plan into a manifest and runs it on polars, pandas or
Spark. Every run writes `run_results.json` with one row per model. It supports incremental
models, merge, replace_partitions and scd2 writes, contracts, schema-change policies,
expectations with quarantine, unit and data tests, retries, timeouts and cancellation.

```console
$ pip install "pipeloom[polars]"   # or pipeloom[pandas]
$ pipeloom validate
$ pipeloom run
```

Documentation: https://emanuel-luis.github.io/pipeloom/latest/

Source: https://github.com/emanuel-luis/pipeloom

License: Apache-2.0.
