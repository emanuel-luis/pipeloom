# {{name}}

A pipeloom project on {{engine}}, created by `pipeloom init`.

- `pipeloom.yml`: the project, its sources, its `dev` target and the `models:` tree, which sets
  the schema of each layer folder.
- `models/bronze`, `models/silver`, `models/gold`: one `@model` function per layer. A parameter
  names the model or source table it reads.
- `models/silver/_properties.yml`: descriptions and settings kept next to the code.
- `pipeloom new model NAME` adds a model file.

```console
$ pipeloom validate
$ {{run_command}}
```

Docs: https://emanuel-luis.github.io/pipeloom/latest/
