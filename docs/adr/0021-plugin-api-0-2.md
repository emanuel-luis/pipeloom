# ADR-0021: Plugin API (0, 2)

Architecture: [Section 5, Engine and catalog contracts](../architecture.md#5-engine-and-catalog-contracts)

Amends [ADR-0002](0002-engine-abstraction.md) and [ADR-0017](0017-plugin-spec-and-builtins.md).

## Context

0.2 is the first release that runs models. The 0.1 engine contract had no way to check what a Python model returned, to share one result between units, to create a view, to say which materializations an engine offers, or to tell whether a state store is visible to other processes. The model runner needs all of them, and the engine plugins (polars, pandas) are built against the same API.

## Options

- **Keep the 0.1 contract and let the runner work around it.** The runner would guess at native types and could not reject a foreign return value with a useful error.
- **Add the missing pieces to the protocols.** One additive change, and core, plugins and the memory engine move together.

## Decision

The plugin API version becomes (0, 2). While the major version is 0 a plugin must match it exactly (ADR-0017), so every plugin is rebuilt for it. The change is additive:

- `Engine.version` names the engine library's version and goes into `run_results.json`.
- `Engine.session(ctx, *, catalog)` receives the catalog. A session is created once per invocation and must be thread-safe, because the thread executor shares it.
- `EngineSession.relation(native)` checks a model's return value and wraps it in a `Relation`. It raises `TypeError` for a foreign type, which the runner reports as PLX101 with a hint.
- `EngineSession.persist(rel)` returns an in-memory copy that later reads reuse. The runner calls it for an ephemeral model that more than one unit inlines.
- `EngineSession.create_view(target, query, inputs)` creates or replaces a catalog view. `inputs` maps the aliases used in `query` to `ReadSpec`s; a persisted model is read as format `table`.
- `EngineCapabilities.materializations` lists what the engine can store: `table`, `view` and `ephemeral` in 0.2. A model whose materialization is missing is PLC509, so `incremental` and `intermediate` are rejected until an engine declares them. Views keep PLC507 for engines without view support.
- `StateStoreCapabilities.shared` says whether every process and task of a run sees the store. `PluginSpec.capabilities` holds either kind of capabilities.
- `ReadSpec.format` may be `table`, a table of the catalog named by `ReadSpec.table`.
- The state store records runs and unit attempts (`record_run`, `list_units`) next to watermarks and locks.
- A view must be a SQL model that inlines no ephemeral model, because the query is stored as written (PLP409).

The runner does not call `EngineSession.filter_since`, `max_value` or `cancel`; they stay for 0.3.

## Consequences

- Plugins built for (0, 1) fail with PLU001 until they are updated.
- The memory engine implements the whole contract, including views, so the contract suites can run against it.
- The runner has one place (`_to_frame_api`) where Narwhals frames will be wired in, for engines that declare `portable`.

## Status

Accepted

## Date

2026-11-05
