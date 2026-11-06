# Run history

`pipeloom runs` lists past runs and shows one run with its units. It reads the state store of the target by default, or the `run_results.json` files under the artifacts root with `--artifacts`. It never starts, changes or locks anything. Decided in [ADR-0047](../adr/0047-run-history-and-webhook.md).

## List runs

```bash
pipeloom runs ls --target dev
```

prints one tab-separated line per run, newest first: run id, status, start time, duration and unit counts.

```text
20261106T101500Z-1a2b	failed	2026-11-06T10:15:00+00:00	12.4s	failed=1 skipped=2 succeeded=5 total=8
20261106T090000Z-9f8e	succeeded	2026-11-06T09:00:00+00:00	10.1s	succeeded=8 total=8
```

- `--limit N` lists at most N runs (default 20).
- `--before RUN_ID` lists the runs older than that one, so passing the last run id of a page gives the next page.
- `--output json` prints a JSON list of run records instead.
- `--artifacts` reads `<artifacts root>/runs/*/run_results.json` instead of the state store. Use it when the store is gone or lives on another machine. A run that is still running, or crashed, has no `run_results.json` and is not listed.

Runs are ordered by start time, then by run id. Only the runs of the selected project and target are listed.

## Show one run

```bash
pipeloom runs show 20261106T101500Z-1a2b --target dev
```

prints the status, the start and end times, the parent run (set by `pipeloom retry --from`) and one line per unit attempt: unit id, attempt and status. `--output json` prints `{"run": ..., "units": [...]}`, and `--artifacts` reads the run's `run_results.json`.

## Errors

| Code | When |
|---|---|
| [PLX117](../reference/errors.md#plx117) | the configured state store has no `list_runs` (a plugin written before plugin API 0.5); use `--artifacts` |
| [PLX118](../reference/errors.md#plx118) | `runs show` or `--before` names a run that this project and target do not have |

## For plugin authors

`StateStore.list_runs(project, target, limit, before=None)` is part of plugin API (0, 5). `pipeloom.state.types.page_runs` orders and pages any sequence of `RunRecord`s, so a store can read its run records and hand them to it. The `StateStoreContract` suite in `pipeloom.testing.contract.state` checks the ordering and the paging.
