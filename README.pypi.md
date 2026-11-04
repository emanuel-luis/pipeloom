# pipeloom

Declarative, engine-agnostic data pipelines in YAML.

pipeloom 0.2 reads a YAML project, validates it in layers, builds the dependency graph, compiles
the execution plan into a manifest and runs it on polars or pandas. Every run writes
`run_results.json` with one row per model. 0.3 adds incremental models, merge and
replace_partitions writes, schema-change policies, and expectations with quarantine.

```console
$ pip install "pipeloom[polars]"   # or pipeloom[pandas]
$ pipeloom validate
$ pipeloom run
```

Documentation: https://emanuel-luis.github.io/pipeloom/latest/

Source: https://github.com/emanuel-luis/pipeloom

License: Apache-2.0.
