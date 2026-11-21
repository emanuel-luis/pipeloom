# ADR-0052: Split checksums and change categories

Architecture: [Section 7, Error codes, and section 11, Artifacts](../architecture.md)

## Context

A node of the 1.0 manifest has one `checksum`. It covers the resolved config, the callable and the bytes of the model's module file, the contract columns and the data tests ([ADR-0042](0042-state-selection-and-defer.md), [ADR-0050](0050-python-first-authoring.md)). `state:modified` can only say that something changed. A reformatted file, a new comment, a fixed docstring and an edited data test all look like a change to the model's logic, and a reviewer cannot tell which. Environment values also land in the checksum, because the config is hashed after `${env:...}` is resolved.

Version 1.2 adds `plan --diff`, which sorts each change into a category. It needs to know which part of a node changed first. This ADR covers that part. The change categories are the planned next step and will be added to this ADR by the PR that adds `plan --diff`.

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

## Status

Accepted. `plan --diff` and its categories will amend this ADR.

## Date

2026-11-20
