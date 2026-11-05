# ADR-0040: Per-model catalog

Architecture: [Section 3, Pipeline](../architecture.md)

Amends [ADR-0022](0022-filesystem-table-format.md).

## Context

A model chooses its schema, but the catalog of its relation comes only from the target: the `catalog` option of the naming strategy, or the `catalog` option of the catalog plugin. A project that keeps each layer in its own catalog (for example `pipeloom_bronze.tpch.orders` and `pipeloom_gold.tpch.revenue_by_region`) cannot say so in YAML. The two path catalogs, `filesystem` and `delta_path`, also ignore the catalog of a target, so a catalog name set on them had no effect.

## Options

- **Where the catalog is resolved.**
  - Pass the model's catalog through the `NamingStrategy` protocol. Strategies could rewrite catalogs, but the protocol changes and the plugin API goes from (0, 4) to (0, 5), which breaks every installed plugin until it is rebuilt.
  - Resolve it in the core, after the strategy returns. The protocol and the API stay. Strategies cannot rewrite a model's catalog.
- **Path catalogs.**
  - Map the catalog to a leading directory: `<root>/<catalog>/<schema>/<name>/`. A target with no catalog keeps the old layout.
  - Add a diagnostic for a catalog on a path catalog. It is explicit, but it needs a new code, and it rejects a layout that has a natural meaning.
  - Keep ignoring it. Nothing changes, but two layers in different catalogs would collide in one directory without a warning.

## Decision

- `ModelSettings` and `ResolvedModelConfig` gain `catalog: str | None`, next to `schema`. It cascades like every other setting: model file, then folder `_defaults.yml`, then `defaults.models`.
- Compile calls `naming.relation(name, schema, identity)` as before. When the resolved model catalog is set, it replaces the catalog of the returned `TargetRef`; when it is unset, the strategy's `catalog` option stays. `NamingStrategy` and `API_VERSION` do not change, and strategies keep transforming only the schema (prefix, per_user).
- `SparkSessionCatalog` and `UnityCatalog` already use `target.catalog or self.catalog` and need no code change. pipeloom creates schemas, never catalogs: a catalog must exist.
- `FilesystemCatalog` and `DeltaPathCatalog` lay a table out at `<root>/<catalog>/<schema>/<name>/` when the target has a catalog, and at `<root>/<schema>/<name>/` when it has none. `ensure_namespace` follows the same rule, and the catalog goes through the same directory-name check as the schema.
- PLP412 (quarantine clash) compares `(catalog, schema, name)`. No new diagnostic code.

## Consequences

- The resolved config gains a field, so node checksums and plan hashes change once, for every project, when it moves to this version.
- A naming strategy that has to rewrite catalogs (for example a per-user catalog) needs the protocol extended and a plugin API bump. It is not needed yet.
- A local Spark session needs each catalog registered in its conf (`spark.sql.catalog.<name>`) before a run; on Databricks the catalogs and the `USE CATALOG` grants must exist.
- Layout change on path catalogs: a project whose naming option set a catalog on `filesystem` or `delta_path` now gets that catalog as a directory. It either moves its tables or removes the option. The CHANGELOG carries the migration note.
- A source with a three-part `table: a.b.c` is split into catalog, schema and name, so on a path catalog it now reads `<root>/a/b/c` instead of `<root>/b/c`. Such a source either moves its files under a directory named after the catalog, or is written as `b.c`.
- PLP412 compares the effective catalog and schema: the model's own value, else the `catalog` and `schema` options of the naming strategy.

## Status

Accepted.

## Date

2026-11-05
