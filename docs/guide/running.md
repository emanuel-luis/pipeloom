# Running a pipeline

`pipeloom run` executes the selected models of a project and writes the run artifacts.

```console
$ pipeloom run --target polars
```

The command validates the project, compiles the plan, takes a run lock, runs every selected unit
in dependency order and finalizes the artifacts. Exit codes: 0 when every unit succeeded, 1 when
a unit or a check failed, 2 for bad usage and 130 when the run was stopped.

Select models with `-s/--select` and `--exclude` (see `pipeloom ls`). A unit whose upstream failed
is skipped with the reason `upstream_failed`. With `--fail-fast` the first failure stops the
scheduler from starting more units; the units still waiting are skipped with the reason
`fail_fast`. Independent branches keep running without it.

## Executors and the boundary

The target's `boundary` decides how units are executed. You can override it with `--boundary`.

| Boundary | Executor | What it does |
|---|---|---|
| `process` (default) | thread | Every unit runs on a thread of the invocation. One engine session serves all of them, and an ephemeral model shared by several units is computed once. |
| `task` | process | Every unit runs as its own `pipeloom run --select unit:<id>` process, so each unit could be an orchestrator task. Events of the child are relayed to the parent, and its logs go to `logs/`. |

`--workers N` (or `PIPELOOM_WORKERS`) sets how many units run at once. The default is the target's
`workers` setting, else the smaller of 4 and the CPU count.

With `boundary: task` the units share nothing in memory, so the state store must be shared
between processes: the `json` store qualifies, `sqlite` does not (PLC503). Starting a child
process costs about a second per unit.

## Run ids and reruns

Each run has an id: `--run-id`, or a timestamp and a short suffix such as
`20261104T014938Z-3fa91c`. Ids use 1 to 64 letters, digits, dots, underscores and hyphens.

Reusing a run id continues that run. Every write carries the idempotency token
`<run_id>:<unit_id>`, and the filesystem catalog remembers the last tokens of each table, so a
unit that already committed under that token is not written twice. Rerunning a finished run
with the same id leaves the data byte-identical. Attempt files keep counting up
(`attempt-2.json`, and so on), and `run_results.json` reports the newest attempt of each unit.

Every engine sees one start time for the run. By default it is the time the run began, and a
reused run id keeps the start recorded the first time. `--run-started-at ISO8601` (or
`PIPELOOM_RUN_STARTED_AT`) sets it instead, for an orchestrator that wants every task to share the
job's start time. The value needs a UTC offset (`2026-11-05T03:00:00+00:00` or a trailing `Z`) and
needs `--run-id`. Giving a different value for a run id that already has a start is PLX108 and
exit code 2. The Spark engine derives its Delta write version from this time.

`--full-refresh` rebuilds append tables from scratch. It takes an optional `true` or `false`, so
an orchestrator can pass a templated value.

To run exactly the plan you compiled earlier, pass `--manifest target/runs/<id>/manifest.json`.
Add `--expect-plan-hash <sha256>` to stop with PLX103 when the plan changed.

## Intermediates

A model with `materialized: intermediate` is persisted for the length of the run, so units in
other processes (`boundary: task`) can read it. The result goes to
`<artifacts.root>/runs/<run_id>/intermediate/<model>` and `relation_name` in `run_results.json` is
that location. An intermediate takes no `incremental` block, no `partition_by` and no strategy
other than overwrite (PLP413).

The `intermediate_ttl` key of the target's `artifacts` block (default `1d`) says how long the data
stays. With `0` the folder is deleted at the end of a run that succeeded; a failed run keeps it.
At the end of every run, finalize also deletes the intermediates of other runs once
they are older than the TTL those runs recorded. A run that left no results, or was resumed and
stopped again, is deleted after it has been idle for the longer of the TTL and one hour. A
deletion that fails is a warning in `run.warnings`, never a failed run.

## Locks

A run takes a lock named `<project>/<target>` in the state store, so two runs of one target do
not overlap. The lock is a lease of 60 seconds that a heartbeat renews every 20 seconds. A run
that dies frees the lock within a minute. A second run that finds the lock held stops with
PLX105 and names the holder, its run id and when the lease expires. `--no-lock` skips the lock.

## Unit mode for orchestrators

`pipeloom run --select unit:<id>` runs a single execution unit and nothing else: no lock, no
`run.*` events, no `run_results.json`. Use it when an orchestrator owns the schedule and the
retries. Each invocation writes its own attempt file and event file under the run folder, and
the invocation that owns the run merges them when it finalizes. The unit ids are in the manifest.

Pass the ids the orchestrator gave the run with `--orchestrator-id KEY=VALUE`, repeated as
needed, for example `--orchestrator-id job_run_id=42`. Keys match `[a-z][a-z0-9_]{0,63}` and
values are 1 to 256 printable characters; a later duplicate wins. The ids go into every attempt
file, the `orchestrator_ids` of each result row and of the run, and the `context` of every event
as `orchestrator_id.<key>`.

### Closing the run: `pipeloom finalize`

When the orchestrator ran each unit itself, nothing merged the results. `pipeloom finalize
--run-id <id>` does it as the last task of the job. For each unit in scope (the whole plan, or
what `-s/--select` and `--exclude` leave) it takes, in this order:

1. the unit's attempt file;
2. else the state the orchestrator reports in `--unit-states PATH`, a JSON file (or the same
   document inline in `--unit-states-json TEXT`, not together with the file),
   `{"schema": "pipeloom.unit_states/1", "units": {"<unit id>": {"status": "failed", ...}}}`.
   `status` is `skipped`, `cancelled`, `failed` or `excluded`; `reason` (for `skipped`, default
   `upstream_failed`), `code`, `message` and `orchestrator_ids` are optional. `excluded` drops the
   unit from the results;
3. else the unit is `skipped` with the reason `upstream_failed` when one of its upstreams failed, otherwise `not_run`, which does not fail the run.

The run is failed when any unit failed, timed out or was skipped as `upstream_failed`, else
cancelled when any unit was cancelled, else succeeded. The exit code is 0, 1 or 130. A file that
cannot be read or does not match the schema is exit 2. Finalize records the run in the state
store, deletes expired intermediates and emits `run.completed`. Running it again with the same
inputs rewrites the same results. It also takes `--manifest`, `--expect-plan-hash`,
`--orchestrator-id`, `--run-started-at` and the project options of `run`.

Finalize refuses with PLX105 (exit 1, as `run` does) while a `pipeloom run` of the same run id is
still live, that is, its lock is held and its run record says `running`.

On a file system without hard links (some network volumes), attempt files are created with an
exclusive open instead of a link, which is still exclusive but not atomic.

## Output

The console prints one line per unit, a summary, the failure block for each failed unit and the
paths of the files written. See [Run artifacts](../reference/artifacts.md) for the files.

Each failure block ends with an `explain:` line. `pipeloom explain <CODE>` (for example
`pipeloom explain PLX101`, any case) prints what the code means and links to its entry on the
[error codes](../reference/errors.md) page.
