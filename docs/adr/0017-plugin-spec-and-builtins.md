# ADR-0017: Plugin spec, built-in plugins and the exact plugin API gate

Architecture: [Section 6, Plugin system and plugin API version](../architecture.md#6-plugin-system-and-plugin-api-version)

Amends [ADR-0004](0004-plugin-discovery.md) and [ADR-0016](0016-versioning-and-plugin-api.md).

## Context

Building the first plugins showed gaps in the contracts those ADRs describe:

- A spec that only carries `name` and `kind` cannot say which group it belongs to, so a mismatch between an entry point and the spec behind it goes unnoticed. L5 also needs to read an engine's capabilities without building the engine, and `plugins list` needs a one-line description.
- Built-in plugins (the memory engine, the naming strategies, the env secret provider) must work when third-party autoload is off, and they must not cost an import when nobody names them. Registering them as entry points of the core distribution would make them depend on how the package was installed.
- ADR-0016 accepts a plugin whose minor version is at most the core's. Before 1.0 any minor may break, so that rule would accept plugins built for an older, different protocol.
- Loading can fail in more ways than PLU001 and PLU002 cover.

## Options

- **Built-ins as core entry points.** Uniform with third-party plugins, but they vanish when autoload is off or the metadata is missing, for example in a vendored or zipped install.
- **Built-ins imported eagerly.** Simple, but every command pays for every built-in, and the memory engine pulls in sqlite3.
- **A lazy in-code table of `module:attr` strings.** Nothing is imported until a config names the kind.

## Decision

- `PluginSpec` gains `group`, `capabilities` and `description`. The fields are now `name`, `kind`, `group`, `api_version`, `factory`, `config_model`, `compatible_engines`, `capabilities` and `description`. `capabilities` is static data: `EngineCapabilities` for engines, `StateStoreCapabilities` for state stores, `None` otherwise. Spec modules must not import heavy libraries at module level, so reading a spec never loads an engine library.
- Built-in plugins come from `pipeloom.plugins.builtin.BUILTINS`, a table of `module:attr` strings by group and kind. They are listed with source `builtin` and the distribution `pipeloom`. They stay available when `PIPELOOM_DISABLE_PLUGIN_AUTOLOAD=1`, which only skips third-party entry points. `--plugin module:attr` always loads, and its module is imported during discovery because the group and kind are only known from the spec.
- While the plugin API major version is 0, a plugin's `api_version` must equal the core's exactly, otherwise PLU001. From 1.0 the ADR-0016 rule applies: same major, minor not above the core's.
- New code PLU003 covers every other load failure: the module cannot be imported, the entry point does not resolve to a `PluginSpec`, or the spec's group or kind differs from the entry point that registered it.
- A kind provided by two entries in one group, built-ins included, is PLU002 once a config names it. `pipeloom plugins list` flags it and exits 1.
- Naming blocks use `kind` like every other plugin block: a `naming` block such as `kind: per_user` with `schema: dev`.

## Consequences

- Built-ins cost nothing until used, and behave the same in every install layout.
- A third-party plugin cannot silently replace a built-in kind; it has to use another name.
- Plugin authors must rebuild against each 0.x core release. That matches ADR-0016's promise that any 0.x minor may break.
- Exact matching rejects a plugin that would still work, which is a deliberate trade until the protocols settle.
- The filesystem catalog is registered now with its options model, and its implementation arrives in 0.2.

## Status

Accepted

## Date

2026-11-04
