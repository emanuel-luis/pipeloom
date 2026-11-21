### Added

- `pipeloom plan --diff --state DIR` ([ADR-0052](docs/adr/0052-split-checksums-and-change-categories.md), 1.2 amendment) sorts each changed model into new, removed, breaking, non-breaking, metadata, indirect-breaking or indirect-non-breaking, with the changed parts and a reason. A SQL change is non-breaking only when sqlglot reads it as projected columns added and nothing else; anything it cannot tell, Python frame code and a reference without split checksums are breaking. `--fail-on breaking|any|none` (default breaking) gates CI, and `--output json` prints `pipeloom.plan_diff/1`, published as `schema/v1/plan_diff.json`.
- Manifest nodes gain an optional `sql{shell, queries}`: the literal `ctx.sql` queries of the model's function and a digest of the rest of its module. It is left out of the plan hash.

### Documentation

- Running guide: "Reviewing a change: plan --diff". CLI reference and artifacts reference list `plan` and `pipeloom.plan_diff/1`.
