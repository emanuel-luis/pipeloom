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

`--full-refresh` rebuilds append tables from scratch. It takes an optional `true` or `false`, so
an orchestrator can pass a templated value.

To run exactly the plan you compiled earlier, pass `--manifest target/runs/<id>/manifest.json`.
Add `--expect-plan-hash <sha256>` to stop with PLX103 when the plan changed.

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

## Output

The console prints one line per unit, a summary, the failure block for each failed unit and the
paths of the files written. See [Run artifacts](../reference/artifacts.md) for the files.
