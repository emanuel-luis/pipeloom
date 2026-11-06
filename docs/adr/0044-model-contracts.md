# ADR-0044: Model contracts

Architecture: [Section 4, Configuration, section 9, Schema change, and section 10, Data quality](../architecture.md)

## Context

A model's output schema is whatever its body returns. `on_schema_change` ([ADR-0026](0026-schema-change-policy.md)) only compares the batch with the existing target, so a first build, an overwrite or a full refresh accepts any schema, and a downstream consumer learns about a changed type from its own failure. dbt calls the fix a model contract: declared columns with a type and a nullability, enforced at build time, where an enforced contract must match the column set exactly ([dbt model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts)). pipeloom already has a logical type vocabulary (`pipeloom.engine.types`: `int64`, `decimal(18,2)`, `timestamp[us, UTC]`, ...) that every engine reports through `EngineSession.schema`, and an `evaluate` primitive that counts nulls the same way on every engine ([ADR-0028](0028-expectation-evaluation.md)).

## Options

- **Column set.**
  - Exact: every declared column is present, no other column is. The written table is what the contract says.
  - Subset: declared columns must be present, others pass. Extra columns reach the table unseen, which is what a contract is meant to stop.
- **Types.**
  - Exact equality of the logical type string.
  - Accept a batch type that widens to the declared one and cast it. Needs a cast step before the schema policy and makes the written type depend on the cast.
- **Nullability.**
  - Read the engine's schema nullability. polars and pandas do not track it, and Spark marks most computed columns nullable.
  - Count nulls with `EngineSession.evaluate` and a `not_null` check per non-nullable column: one aggregation, the same semantics on every engine, and the expectation suite already compares each engine with the reference.
- **Where the check runs.** Before the expectations, or after them on the rows that will be written.
- **Hashes.** Keep `columns` out of the node checksum and plan hash, or add it only when it is declared.

## Decision

- **Config.** A model may declare `columns`, a list of `name`, `dtype` and `nullable` (default true). It is a model key, not a default: `_defaults.yml` and `defaults.models` cannot set it. `dtype` must be a logical type name of `pipeloom.engine.types` (`LOGICAL_TYPE_PATTERN`, also in the JSON Schema); a duplicate column name is rejected. Both are PLS102 at L1.
- **Exact match.** The batch must have exactly the declared column names (case-sensitive, order ignored) and each column's logical type must equal the declared `dtype` string. No widening and no cast: the user casts in the body, so the type is visible where it is produced. Engines report fixed forms (Spark timestamps are always `timestamp[us, UTC]`), so a project that targets several engines declares the form they share.
- **Nullability.** For every column with `nullable: false`, one `evaluate` call counts rows with a null (NaN counts as null, as in expectations). A lazy relation is persisted first, as the quality step does. Engines need the `quality` capability; PLC510 now also covers a non-nullable contract column on an engine without it. Every built-in engine has it.
- **Order.** The contract is checked after the expectations, on the kept rows, and before the schema policy, the quarantine write, `align` and the write. An expectation that drops null rows therefore runs before the contract checks the column. A violation is PLM208, permanent (never retried under `retry_on: transient`), and nothing is written, the quarantine table included.
- **Schema-change policy.** Unchanged. The contract checks the batch; `on_schema_change` still compares the batch with the target. With a contract the batch schema is fixed, so the policy only acts when the target drifted or the contract itself changed.
- **Materializations.** Tables, incremental models and intermediate models are checked. Views and ephemeral models produce no batch of their own, so `columns` on them is PLP414 (L4). Python and SQL models are treated the same.
- **Manifest and hashes.** `nodes[].columns` is an additive field of `pipeloom.manifest/1`, present only when a model declares `columns`. The node checksum and the plan hash cover it only then, so a contract change selects the node under `state:modified` and changes the plan hash, while a project without contracts keeps byte-identical manifests, checksums and plan hashes.
- **Docs site.** A model page shows a Columns table when the node has a contract.
- **Codes.** PLM208 (runtime) and PLP414 (L4) are new in 0.9. PLM209 stays free.

## Consequences

- A contract stops a changed output before any write, on every engine, at the cost of one schema call and, with non-nullable columns, one aggregation per write.
- Exact types are strict across engines: a polars body that yields `timestamp[ns]` fails a `timestamp[us]` contract until it casts. This is deliberate; a widening mode can be added later as an option.
- Lineage, the OpenLineage schema facet and column-level lineage can read `nodes[].columns` without running anything.
- Contracts do not check sources, and they carry no column descriptions yet.

## Status

Accepted

## Date

2026-11-06
