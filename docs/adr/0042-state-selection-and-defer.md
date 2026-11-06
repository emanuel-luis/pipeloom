# ADR-0042: Selection by state and `--defer`

Architecture: [Section 7, Error codes, and section 11, Artifacts](../architecture.md)

## Context

A developer who changes one model wants to build that model and what depends on it, and to read the parents they did not touch from production instead of rebuilding them in their own schema. dbt does this with `--state`, `state:modified`, `result:` and `--defer`. pipeloom has the pieces: `compile --stable` writes a manifest with a checksum for every node and the relation each persisted model is written to, and every run writes `run_results.json`. It has no way to compare against them or to read from them.

## Options

- **How `state:modified` decides a node changed.**
  - Compare the node `checksum` of the reference with this project's. The checksum covers the canonical model config and the body bytes. It already leaves out the relation, which is computed separately from the naming strategy, so a `dev` project and a `prod` reference compare equal when nothing changed. Changing the checksum to drop target values is not needed.
  - The checksum misses rewired inputs, because they live in `PlanNode.inputs`. The comparison key is therefore the checksum plus the sorted alias to unique id pairs.
- **How `--defer` points a read at the reference.**
  - Rewrite the relations of the plan before it runs. This changes `plan_hash` and the manifest, so a deferred run and a plain run of the same project would no longer share a plan. Rejected.
  - Swap the read spec when the runner resolves a read. The plan, its hash and the manifest stay the same. Chosen.
- **Whether `--defer` checks that the reference relation exists.**
  - Check each one first. It costs a catalog round trip per parent and cannot know what this target may read.
  - Trust the reference, as dbt does with `--favor-state`, and let a missing table fail as PLM204 with a hint that names the reference relation. Chosen.

## Decision

- `--state DIR` on `run`, `compile`, `ls` and `graph`. `DIR/manifest.json` is required and `DIR/run_results.json` is optional; the artifacts root (`target`) works as is, because finalize copies both there. A relative `DIR` is relative to the project dir.
- `artifacts/reference.py` loads the reference. A missing folder, an unreadable manifest, a manifest of another project (the target may differ), or run results that are unreadable, have another schema or belong to another project are PLX110 (exit 1).
- `state:modified` selects a node that is absent from the reference or whose key differs, plus the persisted models that inline a modified ephemeral model. `state:new` selects a node absent from the reference. Other values are PLG305. `+` operators work as for any method.
- `result:fail|skip|cancelled|succeeded` matches the status of the model's row in the reference run results: `fail` is failed or timed out, `skip` is skipped for any reason. Ephemeral models never match, as with `unit:`.
- A `state:` or `result:` atom without `--state` is PLG307, and a `result:` atom when the folder has no run results is PLG308. Both are usage errors (exit 2).
- `run --defer` needs `--state` (a click usage error without it). Every persisted model that the run does not build is read from `reference.nodes[uid].relation`. Sources are never deferred, and ephemeral parents are inlined as always.
- An intermediate parent, a parent missing from the reference, or one with no relation there is read from this target, and PLX112 warns. It counts under `--warn-error`.
- PLX111 (exit 2) refuses `--defer` for a unit invocation, for an effective `boundary: task`, and when the reference catalog kind differs from this target's or is missing. `--manifest` runs may defer.
- The run results record `args.state` and `args.defer`. `model.input_read` gains `relation` and `deferred`, both additive.

## Consequences

- Variables and environment values interpolated into a model's config sit inside its checksum, so changing one marks the node modified. A source change is not detected, because a source has no checksum. A Python model's checksum covers its module file, not the modules it imports.
- A manifest records the catalog kind only. With a filesystem catalog, the reference's `root` must be the same as this target's for deferred reads to find the tables.
- Task-boundary defer is refused in 0.8: each unit would run in a child process that does not know the reference. Use `--boundary process`.
- `state:modified` on a project with no changes matches nothing, so PLG303 warns, and a run with `--warn-error` fails.
- Defer trusts the reference. A parent that was never built there, or that this target cannot read, fails the child with PLM204 and a hint that says so.

## Status

Accepted.
