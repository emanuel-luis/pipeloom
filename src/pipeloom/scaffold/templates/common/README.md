# {{name}}

A pipeloom project on {{engine}}, created by `pipeloom init`.

- `pipeloom.yml`: the project and its `dev` target.
- `models/bronze`, `models/silver`, `models/gold`: one example model per layer. Each folder's
  `_defaults.yml` sets the schema of its layer.

```console
$ pipeloom validate
$ {{run_command}}
```

Docs: https://emanuel-luis.github.io/pipeloom/latest/
