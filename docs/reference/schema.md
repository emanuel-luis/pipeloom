# JSON Schema

pipeloom publishes a JSON Schema for each YAML file it reads, so editors can complete keys and
flag mistakes before you run anything.

| File | Schema |
|---|---|
| `pipeloom.yml` | [project.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/project.json) |
| model files under `model_paths` | [models.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/models.json) |
| `_defaults.yml` | [defaults.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/defaults.json) |

## Use it in an editor

Put a modeline on the first line of the file. Editors that run the YAML language server (VS Code
with the Red Hat YAML extension, for example) pick it up:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/project.json
config_version: 1
name: shop
```

Use `models.json` in model files and `defaults.json` in `_defaults.yml`.

## Include plugin options

The published schemas describe the built-in plugins. To get the options of the plugins you have
installed, export your own copy and point the modeline at it:

```console
$ pipeloom schema export --out schemas
```

`--plugins builtin` limits the export to the built-in plugins. Each plugin's options appear as
a branch of the `engine`, `catalog` or `naming` block, selected by `kind`.
