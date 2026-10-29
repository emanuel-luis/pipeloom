# ADR-0004: Plugin discovery via entry points; eight groups

Architecture: [Section 6, Plugin system and plugin API version](../architecture.md#6-plugin-system-and-plugin-api-version)

## Context

pipeloom needs engines, catalogs, state stores, sinks, secret providers, executors, renderers and naming strategies to be supplied by separately installed distributions, without the core importing them.

## Options

- **Entry points** (stdlib `importlib.metadata`, the pytest model). No new dependency.
- **pluggy.** Built for 1:N hook fan-out.
- **A dbt-style namespace import.** Couples plugin layout to import paths.

## Decision

Entry points. pipeloom picks one implementation per role by `kind`, which is 1-of-N selection, not hook fan-out, so pluggy adds a dependency without a matching need.

- Eight groups: `pipeloom.engines`, `pipeloom.catalogs`, `pipeloom.state_stores`, `pipeloom.sinks`, `pipeloom.secret_providers`, `pipeloom.executors`, `pipeloom.renderers`, `pipeloom.naming`.
- Each entry point resolves to a `PluginSpec(name, kind, api_version, factory, config_model, compatible_engines)`.
- Discovery reads metadata only. A plugin is imported only when a config names its `kind`. `pipeloom plugins list` imports nothing.
- `PIPELOOM_DISABLE_PLUGIN_AUTOLOAD=1` restricts loading to `--plugin module:spec`.
- A duplicate `kind` within a group is error PLU002.

## Consequences

- No extra dependency, and listing plugins is safe on locked-down hosts.
- Plugins are versioned against the plugin API version (ADR-0016).
- No hook fan-out. If a later feature needs it, this ADR must be revisited.

## Status

Accepted

## Date

2026-10-29
