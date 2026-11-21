# ADR-0053: Table diff

Architecture: [Section 11, Artifacts](../architecture.md)

## Context

A pull request that changes a model changes the rows it writes. Plan diff (ADR-0052) says which models a change touches. It does not say how their data changes. The 1.2 roadmap asks for `pipeloom diff <model>`: a keyed comparison of one model's relation in two places, with a schema diff, counts and opt-in samples, a versioned JSON document and markdown for a PR comment. Output that can carry rows must show counts and schema only by default, and columns tagged sensitive must be masked.

Provenance: the design comes from public documentation and observed behavior of data diff tools. No code was copied from any project.

## Options

- **What the two sides are.**
  - The model's relation on this target against the same model's relation in a `--state` reference. This is the relation `--defer` reads (ADR-0042): `reference.nodes[uid].relation`. A dev target and a prod reference need no new config. Chosen.
  - A second `--target`. It needs a second bind and compile of the project in one command. Left out until someone asks for it; `compile --stable --target prod` into a folder gives the same reference.
- **How the key is chosen.** `--key COLUMN` (repeatable) wins; otherwise the model's `write.unique_key`. Without either, the command refuses (PLX130). Guessing a key from the contract was rejected: a wrong key makes every count wrong.
- **Where the comparison runs.**
  - `session.sql` on engines with SQL. Null-safe equality is written differently per dialect, pandas has no SQL, and the core cannot import sqlglot to translate.
  - Portable frames through narwhals in `pipeloom.engine.portable`, the one core module that may import it. It covers polars, pandas, DuckDB and Spark with one code path. Rows reach Python only through the session's `count` and `to_rows`, for one row of counts and the samples. Chosen.
- **Numeric tolerance.** Cheap to add, but it needs a flag, a rule for decimals and a test per engine. Left out; equality is exact.
- **Where `sensitive` lives.** An optional `sensitive: bool = false` on the contract column. It is excluded from dumps like `description`, so checksums, plan hashes and manifests stay byte-identical. Masking uses the current project's contract.

## Decision

- `pipeloom diff MODEL --state DIR [--target T] [--key COLUMN]... [--samples] [--output markdown|json]`. The source side is the reference relation of MODEL in `DIR/manifest.json`; the target side is MODEL's relation on the bound target. Both are read with the `table` reader in one session of this target's engine and catalog. Nothing is written.
- PLX130 (exit 2) refuses a model that does not exist, has no relation (ephemeral or intermediate), has no key, has no relation in the reference, or a reference compiled for another catalog kind. PLX131 (exit 1) reports a read or comparison failure, including a key column missing on one side. A bad `--state` folder is PLX110 (exit 1), as for `run`.
- Schema diff by column name: added (target only), removed (source only), type changed. Columns on both sides with the same logical type, key columns excluded, are compared.
- Rows with a null in a key column are counted per side as `null_keys`. Keys that occur more than once on a side are counted as `duplicate_keys`; their rows on both sides are left out of the other counts, so a duplicate key never multiplies rows. The rest are `matched` (every compared column equal), `partial` (some column differs, with a count per column), `source_only` and `target_only`. Equality is null-safe: two nulls are equal, a null and a value differ.
- `pipeloom.table_diff/1`, published as `schema/v1/table_diff.json`, holds the model, key, both relations with row counts, `differences`, the schema diff, the counts, the compared columns and, with `--samples` only, up to 10 rows per kind in key order. A sample value of a sensitive column, key columns included, is `***`.
- The markdown renderer prints counts, the schema diff and the columns that differ. It prints cell values only with `--samples`, masked the same way.
- Exit codes: 0 no differences, 1 differences (any schema change, partial, source-only, target-only, duplicate or null key) or a failure, 2 usage.

## Consequences

- One code path serves every engine whose frames narwhals supports. An engine whose frames it does not support fails with PLX131 instead of a wrong answer.
- Both relations must be readable from this target's catalog, as with `--defer`. On a filesystem catalog the reference's `root` must be the one this target uses.
- Samples sort by key on the engine and fetch at most 10 rows per kind. Counts run as a handful of aggregate queries, so a large table costs a few scans.
- Masking depends on the current contract. A column that was sensitive only in the reference is not masked.
- A second target as the source side, a numeric tolerance and a row limit can be added later without changing the document: new fields stay optional.

## Status

Accepted.

## Date

2026-11-21
