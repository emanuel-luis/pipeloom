# JSON Schema

pipeloom publishes a JSON Schema for each YAML file it reads, so editors can complete keys and
flag mistakes before you run anything.

| File | Schema |
|---|---|
| `pipeloom.yml` | [project.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/project.json) |
| properties files under `model_paths` | [properties.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/properties.json) |

Models themselves are Python functions, so the `@model` signature gives completion and type
checking in the editor. `models.json` and `defaults.json` were removed in 0.9 with YAML model
files; the version folders of 0.8 and earlier keep them.

## Use it in an editor

Put a modeline on the first line of the file. Editors that run the YAML language server (VS Code
with the Red Hat YAML extension, for example) pick it up:

```yaml
# yaml-language-server: $schema=https://emanuel-luis.github.io/pipeloom/latest/schema/v1/project.json
config_version: 1
name: shop
```

Use `properties.json` in properties files.

## Include plugin options

The published schemas describe the built-in plugins. To get the options of the plugins you have
installed, export your own copy and point the modeline at it:

```console
$ pipeloom schema export --out schemas
```

`--plugins builtin` limits the export to the built-in plugins. Each plugin's options appear as
a branch of the `engine`, `catalog` or `naming` block, selected by `kind`.
