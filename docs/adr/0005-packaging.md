# ADR-0005: Packaging

Architecture: [Section 17, ADR-0005 text](../architecture.md#17-adr-list-and-diagrams)

## Context

The core and the engine, orchestrator and lineage integrations have different dependencies and release needs. Users should install only what they run.

## Options

- **One distribution with extras.** Its entry points are registered even when the extra's dependencies are missing, which leads to import errors at load. It also couples dependency bounds.
- **Separate distributions in separate repositories.** High overhead.
- **Separate `pipeloom-<plugin>` distributions in one uv workspace.**

## Decision

The uv workspace.

- The core is distribution `pipeloom` at the repo root. Plugins live in `plugins/pipeloom-<name>/` with import package `pipeloom_<name>` (no namespace packages).
- Convenience extras on `pipeloom` (for example `pipeloom[polars]`) just depend on the plugin distribution.
- PyPI now: publish `pipeloom` 0.0.1 only. Plugin names are published with their first functional release, which avoids PEP 541 placeholder squatting.
- Pending trusted publishers may be registered for the plugin names. Assumption: a pending publisher does not reserve a name (unverified).
- `[tool.uv.workspace]` is added in 0.2 with the first plugin. Whether uv accepts an empty members glob is unverified.

## Consequences

- One repository and one lockfile, with separate dependency bounds per plugin.
- Plugin names stay unclaimed on PyPI until their first functional release, so someone else could register one in the meantime.
- Lockstep versions apply during 0.x (ADR-0016).

## Status

Accepted

## Date

2026-10-29
