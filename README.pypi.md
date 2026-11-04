# pipeloom

Declarative, engine-agnostic data pipelines in YAML.

pipeloom 0.1 reads a YAML project, validates it in layers, builds the dependency graph and
compiles the execution plan into a manifest. Running the plan comes in 0.2.

```console
$ pip install pipeloom
$ pipeloom validate
$ pipeloom compile
```

Documentation: https://emanuel-luis.github.io/pipeloom/latest/

Source: https://github.com/emanuel-luis/pipeloom

License: Apache-2.0.
