# ADR-0056: Run outcome fields

## Context

The 1.3 roadmap asks for outcome fields in `run_results` without a new run or unit status, since a value added to a closed enum breaks `/1` readers ([stability policy](../reference/stability.md)). Four facts shaped the design:

- `skip_reason` is already an open string in the published `/1` schema, in both the results row and the `unit.skipped` event. Nothing needs a new enum value. Only `upstream_failed` and `fail_fast` (or no reason) make a run fail (`status_of`); `not_run` and an orchestrator's own reasons count for nothing.
- A skipped row says `upstream_failed` but not which unit failed. The scheduler and the `unit.skipped` event already know it; the row dropped it.
- A data test with `on_violation: fail` already fails its unit with PLQ303 and skips the descendants while the data stays written (ADR-0045). That is blocking behavior, and it is the 1.0 default. There was no way to fail loudly and still run the descendants.
- The PLQ303 message joins every failing test name, with no bound. Unit test diffs are already capped at 50 rows (`unit_test.failed`); other lists of failing tests in events are bounded by the expectations a model declares.

## Options

- **New statuses (`warn`, `partial`).** Clear, but breaking in 1.x. Rejected.
- **A closed enum of skip reasons in the schema.** Readers could validate values, but a later reason would need `/2`. Rejected; the schema keeps a string.
- **`failed_upstream` as an object (unit and test).** Room for more detail, but readers want a single key to show or group by. A string covers both cases because unit ids and test names are identifiers without dots.
- **`blocking` as a new `on_violation` value.** It would mix two questions (does it fail, does it stop the descendants) in one closed enum. Rejected for a separate boolean.

## Decision

**Skip reasons are an open set with a class each.** The documented values and their class:

| Reason | Class | Emitted by |
| --- | --- | --- |
| `upstream_failed` | failing | the scheduler and `pipeloom finalize` |
| `fail_fast` | failing | the scheduler under `--fail-fast` |
| `not_run` | non-failing | `pipeloom finalize`: no result and no orchestrator state |
| `not_selected` | non-failing | reserved: nothing records unselected units today |
| `up_to_date` | non-failing | reserved for a later `--skip-unchanged` |
| `not_due` | non-failing | reserved for schedules that leave a unit out of a run |
| `upstream_incomplete` | non-failing | reserved for 1.4 partitions whose upstream is not complete |

A null reason on a skipped row is failing, as before. Any other value, an orchestrator's own reason included, is non-failing. Readers must apply the same rule: treat a value they do not know as non-failing. No value is emitted that nothing produces today. The published `/1` schema keeps `skip_reason` as a nullable string and only its description changes; `SkipReason` in `runtime.states` stays the set the scheduler emits.

**`failed_upstream`** is an optional row field, present only on `upstream_failed` rows. It names the root cause, not the direct parent: the unit whose failure skipped this one, or `<unit_id>.<data_test>` when a blocking data test of that unit failed (the first failing blocking test in declaration order; the unit's own row lists them all). `pipeloom finalize` fills it the same way for units it marks `upstream_failed`, carrying the cause down the graph. The `unit.skipped` event keeps `upstream` as the failed unit id, unchanged.

**`blocking`** is an optional boolean on a data test, used only with `on_violation: fail`. Unset means true, the 1.0 behavior. `blocking: false` keeps PLQ303: the unit fails and the run exits 1, but the in-process scheduler releases its children as if it had succeeded and `--fail-fast` does not stop the run for it. The test's `expectations` entry carries `"blocking": false`; the key is absent on every other entry, so 1.0 goldens and readers see no change. The scheduler reads that entry from the unit's row (`failure_cause`), so it works with the thread and process executors alike. `blocking` applies wherever data tests run: `pipeloom build`, and `pipeloom retry` of a build run, which keeps `args.data_tests`. Plain `run` does not run data tests, so it has nothing to block. Under an orchestrator, which runs one `pipeloom run` per unit, the orchestrator's own trigger rules decide whether children run after a failed task, so `blocking: false` has no effect there; `pipeloom finalize` does not count such a unit as failing when it infers `upstream_failed`. `blocking` is dropped from dumps while unset, so node checksums, split checksums and plan hashes of 1.0 projects hold.

**Capped lists.** A message that lists failing test names (PLQ303) lists at most `MAX_LISTED` = 20 and ends with `and N more`. No artifact lists failing row ids; they carry counts.

## Consequences

- Readers find the cause of a skip on the row itself, without walking the graph.
- A team can keep a strict data test red in CI without stopping the rest of the DAG.
- New skip reasons in later 1.x releases need no schema change; readers that follow the class rule keep working.
- `blocking: false` is a local-scheduler feature. Orchestrator renderers could map it to a trigger rule later; revisit with the 1.4 orchestrator work.
- Provenance: designed from this project's own behavior and public descriptions of skip reasons and test severities in other tools; no code was copied.

## Status

Accepted

## Date

2026-11-21
