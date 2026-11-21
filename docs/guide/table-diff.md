# Table diff

`pipeloom diff MODEL --state DIR` compares one model's table in a reference with the same model's
table on your target. Use it before merging a change: build the model in a dev target, then diff it
against what production holds.

```bash
pipeloom compile --stable --target prod --out prod-state/manifest.json
pipeloom run --target dev -s orders
pipeloom diff orders --state prod-state --target dev
```

The source side is the relation the reference manifest records for MODEL, the same relation
`run --defer` reads. The target side is MODEL's relation on the bound target. Both are read through
this target's engine and catalog, so both must be reachable from it. Nothing is written.

## The key

Rows are matched by the model's `write.unique_key`. `--key COLUMN` (repeatable) replaces it, and a
model without either is refused with PLX130.

- Rows with a null in a key column are counted per side as null keys.
- A key that occurs more than once on a side is counted as a duplicate key. Its rows are left out of
  the other counts on both sides, so the command reports the duplicate instead of failing.

## What it reports

- Schema changes by column name: added (target only), removed (source only) and type changed.
- Counts: matched (every compared column equal), partial (some column differs), source only and
  target only, plus a count per column of the rows where it differs.
- Columns on both sides with the same type are compared, key columns excluded. Two nulls are equal;
  a null and a value differ. There is no numeric tolerance.
- NaN follows the engine: polars counts NaN against null as a difference, pandas treats them as
  equal, and DuckDB counts NaN equal to NaN.
- A key column whose type differs between the sides stops the diff with PLX131, since rows cannot
  be matched. Other retyped columns show in the schema changes and are not compared.

The default output is markdown for a pull request comment. It holds counts, column names and
types, never a cell value. `--output json` prints the `pipeloom.table_diff/1` document, published as
[table_diff.json](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/table_diff.json).

## Samples and sensitive columns

`--samples` adds up to 10 rows per kind of difference (source only, target only, partial), in key
order. Tag a contract column `sensitive` to mask its values as `***` in samples:

```python
@model(
    write={"strategy": "merge", "unique_key": ["id"]},
    columns=[
        {"name": "id", "dtype": "int64"},
        {"name": "email", "dtype": "string", "sensitive": True},
    ],
)
def customers(ctx): ...
```

`sensitive` is not part of the checksum or the manifest, so adding it never marks a model modified.

## Exit codes

- 0: no differences.
- 1: a difference (a schema change, a partial, source-only or target-only row, a duplicate or null
  key), or a failure such as a missing table (PLX131) or an unusable `--state` folder (PLX110).
  In CI, tell them apart by the output: differences print a report on stdout, while a failure
  prints nothing on stdout and reports PLX131 or PLX110 on stderr. A PLX131 message names the
  exception type only, never the engine's text, which can quote cell values.
- 2: a usage error, such as an unknown model, a model with no relation or no key, or a reference
  of another catalog kind (PLX130).

The design is in [ADR-0053](../adr/0053-table-diff.md).
