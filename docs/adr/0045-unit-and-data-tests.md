# ADR-0045: Unit tests, post-write data tests and pipeloom build

Architecture: [Section 4, Configuration, section 5, Validation, section 7, Execution, and section 10, Data quality](../architecture.md)

## Context

Expectations ([ADR-0009](0009-expectations-and-quarantine.md), [ADR-0028](0028-expectation-evaluation.md)) judge each batch before it is written, and contracts ([ADR-0044](0044-model-contracts.md)) fix its schema. Neither answers two questions users ask of a transformation. Does the body compute the right rows for a known input, without a warehouse? And does the written table hold together as a whole, for example every foreign key finds its parent? dbt answers the first with unit tests (`given` input rows, `expect` output rows, run on the warehouse without building parents) and the second with data tests (`not_null`, `unique`, `relationships`) run after the build; dbt 1.8 renamed the model key `tests` to `data_tests` to stop the two being confused ([dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests), [dbt data tests](https://docs.getdbt.com/docs/build/data-tests)). `dbt build` runs both in DAG order.

## Options

- **Naming.** A bare `tests` key, or `unit_tests` and `data_tests`.
- **Where cases live.** On the model only, or also in separate files, with or without a new discovery path.
- **Hashes.** Cases in the plan and manifest, or kept out of both.
- **Where a case runs.** Through the full unit runner with a throwaway catalog, or by calling the body directly with in-memory inputs.
- **Comparing rows.** Ordered or as a multiset; with or without float tolerance; in the core or per engine.
- **Data tests.** As expectations with a new action, or as a separate list run against the written table.
- **`build`.** A new scheduler that interleaves test nodes, or `test` followed by `run` with data tests on.

## Decision

### Naming and config

1. `unit_tests` holds cases, `data_tests` holds post-write checks. There is no bare `tests` key. The roadmap text uses these names.
2. A model may declare `unit_tests:`. Any YAML file under `model_paths` may also hold a top-level `unit_tests:` list whose entries name their model with `model:` (a missing `model:` is PLS105). Keeping cases in a `tests/` folder means adding `tests` to `model_paths`; there is no new discovery path and no new project key. `unit_tests` is a model key, not a setting: defaults cannot set it.
3. A case is `{name, given: {alias: {rows | csv, columns?}}, expect: {rows | csv, columns?}}`. `rows` and `csv` are mutually exclusive (PLS108). Every input alias of the model must be given, and only input aliases (PLR208). CSV paths are relative to the project dir and read with the standard `csv` module; an empty field is null. Case names are unique per model (PLR208); an unknown `model:` is PLR208.
4. Cases stay out of `PlanNode`, the manifest, the node checksum and the plan hash. Manifests and plan hashes are byte-identical with or without unit tests.

### Types and comparison

5. A given column's type comes from the given `columns`, else from the upstream model's ADR-0044 `columns`, else from inference. Inline values infer `boolean`, `int64`, `float64`, `string`, `date`, `timestamp[us]` and `timestamp[us, UTC]` from their YAML type (a quoted date stays a string). CSV columns infer `int64`, then `float64`, then `string`; dates and timestamps must be declared. An all-null column with no declared type is PLX115.
6. Comparison lives in `pipeloom.quality.rows`, because the core may not import `pipeloom.testing`; `pipeloom.testing.reference` re-exports it. The compared columns are the keys of `expect`; extra output columns are ignored, a missing one fails the case. Expected values are cast to the output column's logical type. Rows compare as a multiset: order is ignored, duplicates count. Null equals NaN, floats are normalized to 12 significant digits, a naive timestamp is read as UTC, and decimals compare exactly. The diff lists missing and unexpected rows: 10 of each in the message, 50 in the event.

### Running a case

7. Given rows become relations through `EngineSession.from_rows(rows, schema)`. A SQL body runs through the existing `session.sql(body, inputs)`, which scopes the aliases to that query; a Python body is called as `build(ctx, **inputs)` with `is_incremental=False` and the model's `frame_api`. The result is read back with `EngineSession.to_rows(rel)`, which returns plain Python values. No materializer runs, no namespace is created and nothing touches the catalog or the state store. Ephemeral parents are not inlined: their alias is given like any other input.
8. `pipeloom test` takes `--select`, `--exclude`, `--state`, `--target`, `--vars`, `--plugin`, `--project-dir` and `--log-format`. It runs the cases of the selected models in plan order, takes no lock, writes no artifacts and creates no run id. Each case emits `unit_test.passed` or `unit_test.failed`; `unit_tests.completed` closes the invocation with the counts. A case whose rows differ is PLX114, a case that cannot run (a missing or unreadable CSV, a value that does not cast, an untyped all-null column, an engine without `from_rows` or `to_rows`, a body that raises) is PLX115. Both travel as the `code` of `unit_test.failed`, which carries the diff, and count as errors of the invocation. Exit codes: 0 when every case passed, 1 when one failed, 2 for a usage error, 130 when stopped.

### Engine protocol

9. Plugin API (0, 5) gains `from_rows(rows, schema) -> Relation`, `to_rows(rel) -> list[dict]` and `orphans(rel, column, parent, field) -> int` on `EngineSession`. They are additions to 0.5 before 0.9 ships, so a third-party 0.5 engine without them fails only when it is used, with PLX115, mirroring PLX117. `pipeloom.engine.portable` has a narwhals `to_rows` and an anti-join `orphans` that polars and pandas reuse; `from_rows` uses each engine's own logical-type map (polars `polars_dtype`, pandas `arrow_type`), because a narwhals Decimal cannot carry a precision on every backend. `MemoryEngine` keeps plain rows. Spark builds a `StructType` from its logical-type map for `createDataFrame`, reads timestamps back through `unix_micros` so they come out as aware UTC whatever the session time zone, and counts orphans with a `left_anti` join.

### Data tests (implemented in the second half of 0.9)

10. `data_tests: [{name, check: {not_null | unique | relationships: {column, to: {ref | source}, field}}, on_violation: warn | fail}]` is a model key like `columns`. `PlanNode.data_tests` is left out of the manifest when absent; the node checksum and the plan hash cover it only when declared (the ADR-0044 pattern).
11. A relationships violation is a child row whose non-null `column` (NaN counts as null) matches no `field` value of the parent; parent nulls never match. `to` is read like an input, through the runner's read and source specs, so `--defer`, intermediates and per-model catalogs behave the same; an unreadable parent is PLM204. The judge treats `relationships` like `unique`: the violation count is the value. `to` reuses PLR201 and PLR203 for unknown names.
12. Data tests run inside the unit after the write and the watermark commit and before the attempt file, and also on the replay path, so a retry that replays the write token tests again. They read the whole written table, incremental models included. Tables, incremental, intermediate and view models take them; `data_tests` on an ephemeral model is PLP415, as is a relationships `to` that names the model itself, a descendant, an ephemeral model, or that closes a cycle among relationship edges.
13. Results are entries of `results[].expectations` with `scope: "table"`. From 0.9 every entry carries `scope`: batch entries get `"batch"`, filled in when old `commit_meta` replays. The change is additive. `warn` adds to `warnings[]`; a `fail` is PLQ303, permanent: the unit fails, its descendants are skipped, the run exits 1, and the data stays written. PLC510 also covers data tests on an engine without `quality`.

### build

14. `pipeloom build` runs the unit tests of the selected models first; any failure stops it with exit 1 before a run id exists and before any write. Then it runs `run` with data tests on. Relationship parents become extra scheduling edges at run time only (`UnitGraph.from_parents`); `run` and `unit_parent_map` are unchanged. `build` takes `run`'s user-facing flags. `run` gets a hidden `--data-tests true|false` that the process executor passes to its children; `args.data_tests` is recorded in `run_results`, and `retry --from` inherits it.

### Non-goals for 0.9

15. The Airflow and Databricks renderers do not pass `--data-tests` yet. `pipeloom test` does not run data tests against existing tables. `init` templates are unchanged.

## Consequences

- A body is testable on a laptop in milliseconds, on every engine, with the same YAML. The contract suite runs one passing and one failing case per engine.
- Unit tests never change a manifest or a plan hash, so adding them selects nothing under `state:modified`.
- Data tests scan the whole written table on every build, incremental models included; `on_violation: warn` is available where that is too slow.
- Type inference can surprise (a CSV id read as `int64` where a string was meant); declaring `columns` on the given is the fix the guide documents.
- `scope` on every expectation entry changes the run_results goldens and event payloads, additively.
- Third-party 0.5 engines without the new methods fail only when a test runs.

## Status

Accepted

## Date

2026-11-06
