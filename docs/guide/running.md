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

## Retrying a run

`pipeloom retry --from RUN_ID` runs again what that run did not finish: the units that failed,
timed out, were skipped or were cancelled. Fix the model, then retry.

```console
$ pipeloom run --run-id nightly-1        # bad fails, its child is skipped
$ pipeloom retry --from nightly-1        # runs bad and its child, nothing else
```

The retry is a new run with its own id (`--run-id` sets it and must differ from `--from`). Its
`run_results.json` and its state record name the original in `parent_run_id`. The project is
compiled again, so the fix is picked up. The retry inherits the vars (`--vars` wins),
`--full-refresh` (`--full-refresh` or `--no-full-refresh` wins), `--state` and `--defer` of the
run it retries. It takes no `--select` or `--exclude`: the parent's rows already say what to run.
When nothing failed, it prints `run nightly-1 has nothing to retry` and exits 0 without a run
folder.

The parent must be a finished run of the same project and target in the artifacts root;
anything else is PLX113 (for another target, the hint says which `--target` to use). A unit that
failed after its write committed is written again if it appends, because the new run id gives
it a new idempotency token. To continue the same run instead, run it again with its own
`--run-id`.

## Selection by state and defer

Compile production once with a stable manifest and keep it as the reference:

```bash
pipeloom compile --target prod --stable --out state/manifest.json
```

`--state DIR` on `run`, `compile`, `ls` and `graph` reads `DIR/manifest.json` (and
`DIR/run_results.json` when it is there). `target` works too, because every run copies both files to
the artifacts root. A relative `DIR` is relative to the project dir. A missing folder, or a manifest
or run results of another project, is PLX110.

- `state:modified` selects the models that are new or whose body, config or inputs differ from the
  reference, and the persisted models that inline a modified ephemeral model. The comparison ignores
  the target, so a `dev` project and a `prod` reference match when nothing changed.
- `state:new` selects the models the reference does not have.
- `state:modified.code`, `state:modified.config`, `state:modified.contract`, `state:modified.tests`
  and `state:modified.upstream` select by one part of the model
  ([ADR-0052](../adr/0052-split-checksums-and-change-categories.md)). `code` ignores layout,
  comments, docstrings and the `@model` decorator, and a plain SQL string passed to `ctx.sql` has its comments and
  extra whitespace dropped. `config` hashes `${env:...}` values as written, so rotating an
  environment value changes nothing here. `tests` covers the data tests, and `upstream` changes
  when any model above changes. A reference compiled before 1.2 has no split checksums: these
  selectors then compare the whole checksum and warn with PLG309.
- `pipeloom ls --state state --why` prints how each listed model differs, for example
  `orders: code, upstream`. The values are `new`, the parts above, `legacy` (only the pre-1.2
  checksum is known), `legacy only` (the parts agree and only the whole checksum differs) and
  `unchanged`.
- `result:fail`, `result:skip`, `result:cancelled` and `result:succeeded` select by the status of
  the model in the reference run results (`fail` includes timeouts). They need the run results
  (PLG308).

The graph operators work as usual, so `state:modified+` is the models that changed and everything
downstream. A state or result atom without `--state` is PLG307. A change to a variable or an
environment value interpolated into a model's config counts as a change; a change to a source does
not.

`pipeloom run --state state --defer` reads every parent the run does not build from the relation
the reference manifest records for it, so a dev run of one model reads the production upstreams. The
plan and its hash stay the same. The relation is not checked first: a table that is missing, or that
your target cannot read, fails the child with PLM204 and a hint that names the relation. An
intermediate parent, or one the reference lacks, is read from this target and PLX112 warns. `--defer`
is refused (PLX111) for `-s unit:` runs, for `boundary: task` and when the reference was compiled for
another catalog kind. With a filesystem catalog the reference's `root` must be the same as yours.

The dev loop:

```bash
pipeloom run --target dev --state state -s "state:modified+" --defer
```

## Reviewing a change: plan --diff

`pipeloom plan --diff --state state` compiles the project and sorts each model that differs from
the reference into one category
([ADR-0052](../adr/0052-split-checksums-and-change-categories.md)):

| Category | Meaning |
|---|---|
| `new` | The reference does not have the model. |
| `removed` | The project no longer has the model. |
| `breaking` | The data the model writes may change for its readers. |
| `non-breaking` | Readers keep what they had: a projected column added to a SQL query, a nullable contract column added, `cluster_by`, or expectations that drop no rows. |
| `metadata` | Nothing the model writes changes: data tests, `tags`, `meta`, retries or `timeout`. |
| `indirect-breaking` | A model above it is breaking. |
| `indirect-non-breaking` | A model above it is non-breaking, and nothing above is breaking. |

An unknown result never counts as safe. A code change is non-breaking only when the model's
function passes string literals to `ctx.sql`, nothing else in its module changed, and sqlglot
reads the new query as the old one plus projected columns, with no change to the existing
columns, filters, joins or grouping. An added column that may change the row count (an aggregate,
a subquery, a table function or an unknown function) is breaking. Python frame code, SQL that
sqlglot cannot parse, a missing sqlglot, and a reference compiled before 1.2 (PLG309) all count
as breaking. In the contract, dropping or retyping a column, or changing its nullability, is
breaking. In the config, everything not listed above is breaking, for example
`materialized`, `write`, `incremental`, `partition_by`, `schema` and `catalog`.

```text
$ pipeloom plan --diff --state state
model   category               parts     reason
orders  non-breaking           code      added discount
report  indirect-non-breaking  upstream  downstream of orders
```

`--fail-on breaking` (the default) exits 1 when a change is `breaking`, `indirect-breaking` or
`removed`, so the command works as a gate in CI. `--fail-on any` exits 1 on any change and
`--fail-on none` always exits 0. `--select` and `--exclude` narrow the report, and removed models
show only without them. `--output json` prints a `pipeloom.plan_diff/1` object
([schema](https://emanuel-luis.github.io/pipeloom/latest/schema/v1/plan_diff.json)). `--diff` is
the only mode of `plan` for now and is required.

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

Each failure block has an `explain:` line after the hint. `pipeloom explain <CODE>` (for example
`pipeloom explain PLX101`, any case) prints what the code means and links to its entry on the
[error codes](../reference/errors.md) page.

## Working with coding agents

`pipeloom init` writes `AGENTS.md` and three skills under `.agents/skills/`: how to fix a
diagnostic, how to read run results and how to add a model. They are generated from the installed
pipeloom (its commands and its diagnostic catalog) and fetch nothing. After an upgrade, run
`pipeloom init --update-agents` to regenerate them; it touches no other file.

For scripts and agents, add `--log-format json` to read diagnostics as one JSON event per line on
stderr, each with `code`, `hint`, `location` and `explain_url`. `pipeloom compile --output json`
prints the manifest path, its hash and the plan counts as one JSON object on stdout.

## Inspecting a project

These commands write no data. The flags of each are in the
[command line reference](../reference/cli.md).

- `pipeloom validate` runs the checks from YAML syntax to engine capabilities (layers L0 to L6)
  and prints each finding with its code. It exits 1 on an error, and with `--warn-error` on a
  warning too.
- `pipeloom compile` validates the same way, then writes the plan to a manifest (`--out`, default
  `target/manifest.json`). `--stable` blanks the timestamp, the invocation id and the
  selection, so the same project and target always give the same file.
- `pipeloom ls` prints the names of the models that `-s/--select` and `--exclude` choose, one per
  line, or a JSON list with `--output json`.
- `pipeloom graph` prints the selected models in dependency order as text (`name <- parents`),
  Graphviz `dot`, Mermaid or JSON. `--units --output json` prints the execution units instead; see
  the [orchestrator protocol](orchestrators.md).
- `pipeloom plugins list` lists the installed plugins by group and kind from their metadata,
  without importing them. `--group` keeps one group, `--output json` prints JSON, and `--check`
  imports each plugin and adds its plugin API version and a status. A kind that two
  distributions provide is PLU002 and exit 1.
- `pipeloom schema export --out DIR` writes the JSON Schemas of `pipeloom.yml` and the
  properties files; see [JSON Schema](../reference/schema.md).
