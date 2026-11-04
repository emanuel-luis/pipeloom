# ADR-0026: Schema-change policy

Architecture: [Section 9, Incremental processing, write strategies, schema change](../architecture.md#9-incremental-processing-write-strategies-schema-change)

Amends [ADR-0008](0008-incremental-key-watermark-strategies.md).

## Context

`write.on_schema_change` had four named policies and no rules. A batch can add, drop or retype columns, and the answer depends on what the strategy does with the rows already in the table. The core must decide the final schema without asking the engine, and every engine must end up with the same table.

## Options

- **Let each engine decide.** Simple for plugins, but the same model would evolve differently on polars and on the memory engine.
- **The core decides, the engine applies.** One pure function picks the final schema from the target schema, the batch schema, the policy and the strategy. Engines only align a frame to a schema and read old data through the table's current schema.

## Decision

The core decides. `pipeloom.runtime.schema_policy.resolve_columns(policy, target, batch, *, strategy, full_refresh)` returns the final schema, a `SchemaDiff` and an action (`applied`, `ignored`, `rejected`, `replaced` or `none`). The runner passes the final schema as `WriteConfig.columns` and aligns the batch with `EngineSession.align` first.

The policy applies only when the strategy keeps existing rows (append, merge, replace_partitions), the target exists, and the run is not a full refresh. Overwrite, full refresh and a first build use the batch schema; a non-empty diff is still reported, with action `replaced`.

- `fail`: any change is PLM202.
- `ignore`: the final schema is the target's. The batch is aligned to it: extra columns dropped, missing ones filled with nulls, values cast to the target types. A cast that fails is PLM201.
- `append_new_columns`: the target's columns followed by the added ones, in batch order and nullable. Removed columns stay. A type change is PLM202.
- `sync_all_columns`: the batch's columns. A type change is accepted only when `can_widen(target, batch)`; otherwise PLM202.

A batch whose types differ from the target only by a lossless widening of the target type (`diff_schemas` reports no change) is cast up to the target's types under every policy.

Widening follows `can_widen`: integer to a wider integer or to float64, float32 to float64, decimals that keep every digit, finer timestamp units with the same zone, and the null type to anything.

Every non-empty diff emits `model.schema_changed` with `relation`, `policy`, `action` and `changes`, and is copied to `schema_changes[]` of the unit's `run_results` row (each change carries its action). A rejected change is emitted at warning level before the unit fails. The console prints one line per event. Architecture section 9 named the event `schema.changed`; section 11 and the code use `model.schema_changed`.

Tables with files, such as the filesystem catalog, read every committed file through the table's current schema, so append and widening never rewrite old files. An engine that cannot do that yet leaves `EngineHarness.schema_on_read` false and the contract matrix skips the cells that write a changed schema.

## Consequences

- One matrix in `EngineContract` (4 policies, 4 kinds of change, 3 strategies) checks the final schema and rows of every engine against the reference.
- A narrower batch type never fails a run; it is widened silently to the target.
- Dropping a column with `sync_all_columns` also drops its old values on strategies that rewrite rows (merge, replace_partitions); on files it only hides them from readers.
- Removing the need for `schema_on_read=False` is the work of the filesystem merge and replace_partitions change.

## Status

Accepted

## Date

2026-11-05
