# ADR-0028: Expectation evaluation

Architecture: [Section 10, Data quality](../architecture.md)

Amends [ADR-0009](0009-expectations-and-quarantine.md).

## Context

Expectations have three actions (warn, drop, fail) and two families of check: row checks and aggregate checks. Every engine must count the same violations, and the run must report them the same way, or a model changes meaning when its engine changes.

## Options

- **Engines judge.** Each engine returns pass or fail. Ratios, messages and events would then differ per engine.
- **Engines count, the core judges.** Engines return raw counts and aggregate values. The core decides pass or fail, the drop ratio, PLQ301 and the events.

## Decision

Engines count, the core judges (`pipeloom.quality.judge`). `EngineSession.evaluate` returns the total, violating rows per row check, rows violating at least one drop check, and aggregate values. `apply_drops` splits the rows. A pure-Python reference (`pipeloom.testing.reference`) defines the semantics and the contract suite compares every engine against it.

Row checks: a null input passes (SQL CHECK semantics) except for `not_null`; NaN counts as null; `range` bounds are inclusive; `regex` is an unanchored search in the common subset of Rust regex and `re`; `accepted_values` compares exactly. An `expr` is an engine-dialect string: true passes, false fails, null passes. Float columns have NaN turned into null before it is evaluated. What it may contain depends on the engine, and it never runs code:

- polars: a SQL expression for `pl.sql_expr` (column names, literals, comparison, boolean and arithmetic operators, `IN`, `BETWEEN`, `LIKE`, and the SQL functions polars supports). It sees the columns only.
- pandas: a restricted Python expression for `DataFrame.eval(engine="python")`. Only column names, literals, comparison, `and`, `or`, `not`, `&`, `|`, `~`, arithmetic, `in` and `not in` with a tuple or list are accepted. `@name`, function and method calls, attribute access, subscripts, comprehensions and conditional expressions are rejected before evaluation, and the evaluation gets no local or global names (a config file must not run code). A rejected expression is PLQ302. The same rule applies to a merge `update_condition`. A pandas frame backed by numpy floats cannot hold a null, so there a NaN compares as false.
- memory: a sqlite expression over the row's columns.

`regex` needs a string column; any other type is PLQ302, because engines render non-strings differently.

Aggregate checks (`unique`, `row_count`, `freshness`) see only the batch, after the drops. For an incremental model that is the new rows, not the table. `unique` counts rows minus distinct keys and leaves out rows with a null key part. Freshness is `now - max(column)` computed by the core with the run's clock; a naive timestamp reads as UTC and an all-null column fails. An aggregate check cannot drop (PLP403).

Row counts are taken on the full output, so a failed drop check is still visible. The runner persists a lazy relation first (`cached: true` in the event), counts, drops if a drop check failed, then counts the aggregates on what is kept. When nothing can drop, one `evaluate` call counts everything.

A failed check with action fail, or a drop ratio above `quality.max_drop_ratio`, fails the unit with PLQ301 before it writes. The counts are still recorded: `model.quality_evaluated`, and `expectations[]`, `rows_dropped`, `rows_quarantined` and `warnings[]` in `run_results`. Warnings keep the unit succeeded and print WARN lines on the console. An unknown column or an `expr` the engine rejects is PLQ302.

Quarantine: with `quality.quarantine` the dropped rows are appended to `<target>__quarantine` under the token `<run_id>:<model>#quarantine`, so a replay writes them once. `rows_quarantined` is 0 until that write has committed: a unit that fails with PLQ301 or on a rejected main schema reports no quarantined rows. The table has the model columns plus `_pipeloom_run_id` (string), `_pipeloom_failed` (list of the failing drop-check names, in declaration order) and `_pipeloom_ts` (timestamp[us, UTC]). On a csv catalog `_pipeloom_failed` is JSON text in a string column. Its schema policy is `append_new_columns`: new model columns are added and removed ones stay, so a changed model output keeps working. A column that changes type incompatibly is the one thing it cannot absorb: the unit fails with PLM202 naming the quarantine relation (a `model.schema_changed` event for that relation, `schema_changes[]` of the model untouched), before the main table is written. Any other quarantine write error is PLM201 naming the quarantine relation. The write happens after the schema policy accepted the main write and just before it.

Static checks: PLP411 (expectations on a view or ephemeral model, declared by the model itself), PLP412 (`<model>__quarantine` is also a model in the same schema), PLC510 (engine without the `quality` capability).

## Consequences

- Every engine reports the same counts; the dialect strings are the one place engines may differ.
- A model with a failed fail-expectation leaves its target and its quarantine untouched.
- Quarantine warnings are appended to `warnings[]`; they never replace warnings recorded earlier in the unit.
- The memory JSON table store writes dates and datetimes as text, so a quarantine table kept in it cannot be appended to by a second run (the schema reads back as string). Real catalogs keep the types.

## Status

Accepted

## Date

2026-11-04
