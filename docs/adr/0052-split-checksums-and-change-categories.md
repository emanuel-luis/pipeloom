# ADR-0052: Split checksums and change categories

Architecture: [Section 7, Error codes, and section 11, Artifacts](../architecture.md)

## Context

A node of the 1.0 manifest has one `checksum`. It covers the resolved config, the callable and the bytes of the model's module file, the contract columns and the data tests ([ADR-0042](0042-state-selection-and-defer.md), [ADR-0050](0050-python-first-authoring.md)). `state:modified` can only say that something changed. A reformatted file, a new comment, a fixed docstring and an edited data test all look like a change to the model's logic, and a reviewer cannot tell which. Environment values also land in the checksum, because the config is hashed after `${env:...}` is resolved.

Version 1.2 adds `plan --diff`, which sorts each change into a category. It needs to know which part of a node changed first. This ADR covers that part. The change categories are in the 1.2 amendment below.

The 1.x stability rules ([ADR-0016](0016-versioning-and-plugin-api.md)) allow only additive manifest fields. The 1.0 `checksum` and the plan hash must stay byte-identical for an unchanged project.

## Options

- **Where the split lives.**
  - Change the 1.0 `checksum`. Every 1.0 reference would look modified and plan hashes would move. Rejected.
  - Add optional `checksums` and `checksum_version` next to it, and keep them out of the plan hash. Chosen.
- **How `code` ignores layout.**
  - Hash the function body only. Editing a helper in the same module would then select nothing, and an unknown result must never count as safe. Rejected.
  - Hash the whole module's syntax tree. Comments and layout are not in the tree. Docstrings of the module, classes and functions are removed, and `@model` decorators are dropped because what they set is config, contract, tests or inputs, which other parts cover. Chosen. This keeps the 1.0 granularity: an edit to any function of a module marks every model of that module.
  - The tree is serialized by pipeloom itself, without positions and without fields that are None or empty, so a field a later Python adds with an empty default does not move the digest. `ast.dump` changes its output between Python versions and was not used.
  - SQL text has no tree in the core (sqlglot is not allowed there). When the first argument of a `.sql(...)` call is a plain string, its `--` and `/* */` comments are removed and its whitespace is collapsed outside quotes. Text that this cannot read safely is left as is: an f-string (its pieces cannot be read one at a time), text with a backslash (escapes) or a `$` (dollar quoting), and text whose quotes do not balance once the quoted literals are taken out. SQL kept in a variable is not normalized either. In each of these cases a layout change still counts as code, which is the safe side.
- **What `config` hashes.**
  - The resolved config, as 1.0 does. It holds environment values, which may be secrets passed through `${env:...}`. Rejected.
  - The resolved config with every value that `${env:...}` filled put back as written in the YAML, with `${var:...}` resolved. Chosen. `${secret:...}` is never resolved at compile time, so secrets are never hashed. The settings layers of each model are kept on the resolved model, so the winning layer of each value is known.
- **What `upstream` covers.**
  - Only the inputs. A parent's change would not reach its children. Rejected.
  - The sorted inputs (alias and unique id) with each model parent's `code`, `config`, `contract` and `upstream` digests. A change propagates to every descendant. A parent's `tests` are left out, since a test does not change the data. A source counts by its id only. Chosen.

## Decision

- Manifest nodes gain `checksums{code, config, contract, tests, upstream}` (sha256 hex digests) and `checksum_version` (1). Both are optional in the `pipeloom.manifest/1` schema, absent in 1.0 manifests, and left out of the plan hash. The 1.0 `checksum` is unchanged.
- `contract` hashes the contract columns, `tests` the data tests (null when the model declares none). Unit tests stay out of the manifest and of every checksum.
- `state:modified.code`, `.config`, `.contract`, `.tests` and `.upstream` select the models that are new or whose part differs, plus the persisted models that inline a selected ephemeral model, as `state:modified` does. `state:modified` keeps its 1.0 meaning.
- A reference node without `checksums`, or with another `checksum_version`, falls back to the legacy key (checksum plus inputs) for a sub-selector. The new warning PLG309 counts those models once per atom.
- `ls --why` (with `--state`, text output only) prints `name: parts` per model: `new`, the parts that differ in the order above, `legacy` when only the 1.0 key can tell, `legacy only` when the parts agree and the 1.0 checksum does not (a layout, comment or docstring edit, or an environment value), or `unchanged`. `--why` without `--state`, or with `--output json`, is a usage error. Machine-readable output for these commands comes later in 1.2.
- Provenance: designed from the pipeloom code base, the Python `ast` documentation and public descriptions of state comparison in other tools. No code was copied from any other project.

## Consequences

- A formatting pass over a project selects nothing under `state:modified.code`, while `state:modified` still selects every touched model.
- Rotating an environment value changes the 1.0 checksum but no split checksum.
- A `${var:...}` value that itself came from `${env:...}` in the `vars:` block is hashed resolved, as the var record does not say where its value came from.
- When `${env:...}` fills any `tags` value, the config hashes the tag list of every layer as written, in layer order, since tags are a union of layers.
- A non-finite float literal such as `1e999` is hashed as its text, since canonical JSON has no infinity.
- Changing the normalization later bumps `checksum_version`, and references written before fall back with PLG309 until they are compiled again.

## Amendment (1.2): plan --diff and change categories

Decided on 2026-11-21 with the `plan --diff` change (roadmap 1.2).

### Context

The split checksums say which part of a model changed. A reviewer also wants to know what the change does to the data readers see, and CI wants to stop a change that may break them. A category must never call a change safe when pipeloom cannot tell.

### Options

- **Where the old SQL comes from.** The reference is a manifest, not a checkout, so the old module is gone.
  - Treat every code change as breaking. Then no code change can be non-breaking, which the roadmap asks for. Rejected.
  - Record the literal `ctx.sql` queries in the manifest. Chosen. A node gains an optional `sql{shell, queries[{sql, dialect}]}`, left out of the plan hash like `checksums`. `queries` are the string literals passed to `ctx.sql` inside the model's function, in tree order, with each call's literal `dialect=`. `shell` is the digest of the module tree (normalized as for `code`) with the queries of every model function of the module blanked, combined with the callable. It says whether anything but model SQL changed. A helper's SQL or any Python edit changes it; a sibling model's SQL does not, since that SQL belongs to the sibling. The module is parsed and hashed once, not once per model: a module with 1,000 models stays linear. A function with no such call, or with a `dialect=` that is not a literal, gets no record.
- **Where sqlglot runs.** The import-linter contract allows it only in `validate.static`. The comparison `sql_change` lives there, the classifier in `artifacts.plan_diff` calls it, and the contract is unchanged. Without sqlglot it answers breaking.
- **The command surface.** `plan` did not exist. It is added with `--diff` as its only mode, required, so a later mode can be added without changing what `plan` alone means. Rejected: a flag on `ls` or `compile`, whose output and exit codes would change meaning.

### Decision

- `pipeloom plan --diff --state DIR [--select ...] [--exclude ...] [--fail-on breaking|any|none] [--output text|json]` compiles the project, loads the reference as `--state` does elsewhere (PLX110 on a bad folder), and reports each changed model. Unchanged models are left out.
- Own category, from the parts that differ (the worst wins, breaking over non-breaking over metadata):
  - Changed inputs: breaking. `upstream` alone gives no own category; parents decide.
  - `tests`: metadata.
  - `contract`: a dropped or retyped column, a nullability change, a contract added or removed, or an added column that is not nullable is breaking. An added nullable column, or only a new column order, is non-breaking.
  - `config`, by top-level key: `tags`, `meta`, `retries`, `retry_delay`, `retry_on` and `timeout` are metadata, because they change how a run goes and not what it writes. `cluster_by` is non-breaking, because it changes layout and not rows. `expectations` are non-breaking unless a changed expectation has `on_violation: drop`, which removes rows. Every other key is breaking, including `materialized`, `write`, `incremental`, `partition_by`, `quality`, `frame_api`, `schema`, `catalog`, `sql_dialect` and any key added later. A config digest that differs while the resolved config does not (an `${env:...}` text change) is breaking.
  - `code`: breaking unless both sides have a SQL record with the same shell and the same number of queries, and each query pair is the same or a pure addition. sqlglot parses both with the call's dialect, else the model's `sql_dialect`, else the engine's. A pure addition is one SELECT statement on each side where the old projections appear in the new in the same order, the rest of the query is identical, the old query is not DISTINCT, grouped or aggregate, has no `*`, and each added column has a new name and no aggregate outside a window, subquery, table function, UNNEST or unknown function. Queries are not qualified against a schema. Anything else, a parse error, more than one statement, or a missing sqlglot is breaking.
  - A reference node without split checksums of this `checksum_version` falls back to the legacy key: any difference is breaking, with part `legacy`, and PLG309 counts those models once.
- Categories: `new`, `removed`, `breaking`, `indirect-breaking`, `non-breaking`, `indirect-non-breaking`, `metadata`. A model below a breaking model is indirect-breaking unless it is breaking or new itself. A model below a non-breaking model is indirect-non-breaking unless its own change ranks higher. The reason names the models above. Indirect categories follow the current graph, ephemeral models included.
- `--select` and `--exclude` (state selectors included) narrow the report. Removed models appear only without them, since they cannot be selected.
- Exit codes: 0 when nothing reaches the `--fail-on` level, 1 otherwise or on errors, 2 on usage errors. `breaking` (the default) counts `breaking`, `indirect-breaking` and `removed`; `any` counts every change; `none` never fails.
- Output: text is one row per model (model, category, parts, reason) or `no changes`. JSON is `pipeloom.plan_diff/1` (`project`, `target`, `fail_on`, `failed`, `counts` per category, `changes[{unique_id, name, category, parts, reason}]`), published as `schema/v1/plan_diff.json`.
- Provenance: designed from the pipeloom code base, the sqlglot documentation and public descriptions of change categories in other tools. No code was copied from any other project.

### Consequences

- SQL text now appears in the manifest. It was already in the project's source, and the manifest already names the module.
- A manifest compiled by 1.2 before this change has split checksums but no SQL record: its code changes are breaking until the reference is compiled again.
- A model function that calls another model function directly would not see that function's SQL edit in its shell. Models are not meant to call each other; inputs are the way to read another model.
- Positional readers of a table may still see a column added in the middle of a SELECT as a change. pipeloom writes by name, so this is non-breaking here.

## Status

Accepted. Amended in 1.2: `plan --diff` and change categories (see above).

## Date

2026-11-20
