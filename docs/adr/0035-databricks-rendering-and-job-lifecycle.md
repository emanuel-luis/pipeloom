# ADR-0035: Databricks rendering and job lifecycle

## Context

pipeloom 0.5 deploys a plan as a Databricks job. ADR-0011 sketched the job shape and ADR-0012 the orchestrator modes, and both left facts to verify. ADR-0034 gave the core the hooks (a `deploy` block, a stable manifest, plugin commands, `pipeloom finalize`). This ADR covers the plugin side. It is written in halves: this half is rendering, which is pure and needs no workspace; the second half (deploy, run, repair, find by tag, upload, task values) arrives with the client and amends this record. It amends ADR-0011 and ADR-0012.

Facts checked against the public Jobs API documentation: a task key is 1 to 100 characters of `[A-Za-z0-9_-]`; jobs/create, reset, update and runs/submit all accept at most 1,000 tasks; a job has at most 10 environments; an environment has an `environment_version` (a string, 1 to 6) and `dependencies`; a job run's result comes from its leaf tasks.

## Options

- **A `databricks` executor.** ADR-0020 picks executors by boundary, and Databricks is an orchestrator, not a boundary. Rejected.
- **Per-model compute as a model key.** Compute is a runtime setting a target may override, so a model key would need its own precedence. Rejected for target-level `compute_rules` with selectors.
- **Grouping only above the task limit.** The shape of the job would change when the 1,001st model is added. Rejected: grouping applies whenever `task_grouping` is set.
- **Tasks call the core CLI directly.** Needs an environment where the `pipeloom` script exists, and cannot turn a start time or an empty run id into arguments. Rejected for a plugin wrapper entry point.
- **Rendering through the SDK dataclasses.** They lag the API and drop fields such as `environment_version`. Rejected for stdlib frozen dataclasses that mirror the subset we use.

## Decision

The plugin `pipeloom-databricks` registers the `databricks` kind in the `renderers` group. Its options (`DatabricksDeployOptions`) are the validated `deploy` block: pipelines (each a selection of units and one job), named computes (`serverless` or `job_cluster`), `compute_rules` (the first matching rule wins), `task_grouping` (`none`, `by_tag`, `by_folder`) and passthrough job `settings`. The spec imports no Databricks or Spark library. Rendering is a pure function of the plan, the options and a few locations the caller supplies (project root, manifest path, the target's artifacts root, whether its state store is shared, `cancel_grace`); the core needs no new input for it.

A job is named `pipeloom_<project>_<pipeline>_<target>` and carries the tags `pipeloom_job` (`<project>.<pipeline>.<target>`), `pipeloom_plan_hash`, `pipeloom_settings_hash` (sha256 of the canonical settings without that tag) and `pipeloom_version`. User tags are merged and ours win. Equal settings hashes mean a deploy is a no-op.

Tasks run a plugin wrapper: `python_wheel_task` with package `pipeloom_databricks` and entry point `pipeloom_databricks_task`. Parameters are `--opt=value` strings, so an empty value stays one argument. They carry dynamic references (`{{job.parameters.pipeloom_run_id}}`, `{{job.id}}`, `{{task.name}}`, `{{job.start_time.timestamp_ms}}`); the start time is passed as milliseconds because the `iso_datetime` format is undocumented. A one-time run (`runs/submit`) gets the run id, the full-refresh flag and the start time as literals and no job references. It may use serverless compute only.

Each unit is a task named by its unit id; with grouping, a group is a task named `tag_<first tag>` (`tag_untagged`) or `dir_<folder of the model file>` (`dir_root`). Group dependencies come from unit parents; groups that wait for each other are PLD003. A group must run on one compute. A finalize task, `pipeloom__finalize`, depends on every leaf, runs with `run_if: ALL_DONE`, one retry and a 1,800 s timeout, on the default compute. Tasks are sorted by key, finalize last. Retries and the retry delay are the model's (a group takes the largest); `timeout_seconds` is the model timeout plus `cancel_grace` plus 300 s, summed over a group whose every member has a timeout.

Task keys replace characters outside `[A-Za-z0-9_-]` with `_`. A key above 100 characters, a key shared by several ids and a key equal to the finalize key are shortened to 91 characters, `_` and 8 hex characters of the sha256 of the raw id; every id in a collision gets the suffix, so keys do not depend on order. A job above 1,000 tasks, finalize included, is PLD001 with a hint to set `task_grouping` or split pipelines. PLD002 stays reserved and is never emitted, because submit and create share the limit. Invalid options for the plan or target are PLD004 (environment version 1, an unknown compute, more than 10 serverless environments, a key in `settings` that pipeloom owns (`name` too) or a `pipeloom_` tag, a `${secret:` reference, an artifacts root that is unset or relative, a state store that is not shared, a group on two computes, an ephemeral run on a job cluster, an unknown pipeline or one that selects no unit). A compute rule that matches no model is the warning PLD005.

Output is byte-deterministic: JSON with 2-space indent, ASCII only and LF, keys in field order; bundle YAML from `yaml.safe_dump` without key sorting. Environment dependencies default to the pinned distributions (`pipeloom`, `pipeloom-databricks` and every plugin in the plan, `name==version`, sorted).

## Consequences

The job is a plain JSON document that a person can read, diff and send to the Jobs API unchanged, and rendering is testable with goldens and no workspace. The renderer needs three inputs the plan does not hold (artifacts root, state sharing, cancel grace), which the caller reads from the bound target. Tasks of a pipeline ignore dependencies on units outside it. Platform behaviors that only a live run can confirm (references in wheel task parameters, `only` with an `ALL_DONE` finalize) are guarded by the wrapper and checked by the manual workflow, not here.

## Status

Accepted (first half: rendering).

## Date

2026-11-05
