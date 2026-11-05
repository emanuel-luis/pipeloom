# pipeloom-databricks

The Databricks renderer for [pipeloom](https://github.com/emanuel-luis/pipeloom). It turns a
compiled plan into Databricks Jobs settings (JSON for the Jobs API) and into a bundle YAML file.

```console
$ pip install "pipeloom[databricks]"
```

Describe the deployment in a target:

```yaml
targets:
  prod:
    engine: {kind: spark}
    catalog: {kind: spark_catalog}
    state: {kind: table, table: ops._pipeloom_state}   # shared by every task
    artifacts: {root: /Volumes/main/ops/pipeloom}
    deploy:
      kind: databricks
      pipelines: {main: {}, daily: {select: ["tag:daily"]}}
      compute:
        default: {kind: serverless, environment_version: "5"}
      task_grouping: none        # none, by_tag or by_folder
```

- One job per pipeline, one task per unit (or per group of units), plus a finalize task that
  closes the run. A job above 1,000 tasks is an error (PLD001).
- Rendering is pure: no network, no Databricks SDK call and no credentials are needed. The
  output is byte-for-byte the same for the same plan.
- Compute is serverless environments or job clusters, chosen per model with `compute_rules`.
- A pipeline job runs only its own units. An upstream model in another pipeline is read from
  its persisted output, so schedule that pipeline first.
- Diagnostics: PLD001 (task limit), PLD003 (group cycle), PLD004 (invalid deploy options),
  PLD005 (compute rule that matches no model), PLD013 (unresolved task parameter).
- Bundles: `pipeloom databricks bundle --target prod --out resources` writes one job file and
  one manifest per pipeline into a Declarative Automation Bundle (the output folder must lie
  inside the bundle, next to `databricks.yml`; add the job files to its `include`). Paths use
  `${workspace.file_path}`. `--check` lists stale files and exits 1.
- Tasks run `pipeloom_databricks_task`, a wrapper that calls pipeloom in the task's own process,
  so an existing Spark session is reused. A parameter that Databricks left as `{{...}}` stops
  the task with PLD013. After a unit task the wrapper sets the task value `pipeloom`, a small
  summary of its units (best effort).
- `pipeloom_databricks.testing.jobsim.simulate_job` runs a rendered job locally for tests.
- Direct deploy: `pipeloom databricks [--profile NAME] deploy --target prod [--pipeline NAME]...
  [--dry-run]` uploads the project to the workspace (content addressed, under
  `<workspace_root>/<project>/<pipeline>/<target>/<hash>`) and creates or resets one job per
  pipeline, found by its `pipeloom_job` tag. It prints one JSON line per pipeline. `--dry-run`
  prints the job settings and connects to nothing. The upload holds `pipeloom.yml`, the model
  files and the `*.py` files below `python_paths` (virtual environments, `dist`, `build` and
  similar folders are skipped). Other files below `python_paths` are not uploaded, so a Python
  model that reads a file next to it (`.sql`, `.json`, `.csv`) works locally but not on
  Databricks; keep such data in a table or a volume. Notebooks and files above 10 MB are refused
  (PLD012).
- Connection: `--profile`, else a `DATABRICKS_CONFIG_PROFILE` you set, else `DATABRICKS_HOST`
  with credentials, else the Databricks runtime, else PLD010. pipeloom never picks a profile
  by itself. Other codes: PLD006 (two jobs with the same tag), PLD009 (API error).
- `pipeloom_databricks.api` is the stable surface for other distributions, and
  `pipeloom_databricks.testing.fake.FakeDatabricks` a fake workspace for tests.
- Run: `pipeloom databricks run --target prod [--pipeline NAME] [--select S]... [--exclude S]...
  [--run-id RID] [--full-refresh] [--ensure] [--ephemeral] [--wait|--no-wait]` starts the
  deployed job (PLD007 when none is deployed, PLD008 when it differs from the local render,
  unless `--ensure` deploys first). Selected units map to task keys plus finalize (`only`); a
  partly selected group runs whole, with the warning PLD015. `--timeout SECONDS` (default 24
  hours) bounds `--wait`. `--run-id` names the run and makes the start
  idempotent. `--ephemeral` uploads the project and submits a one-time run on serverless
  compute. It prints one JSON line with the ids and the run page URL, then with `--wait` (the
  default) polls every 15 seconds, reports task transitions and the failed units, and exits 1
  unless the run succeeded.
- Repair: `pipeloom databricks repair --target prod (--run-id RID | --job-run-id N)
  [--wait|--no-wait]` reruns the tasks that failed, timed out, were cancelled or skipped, plus
  finalize. A run that is still active is PLD014; nothing to repair exits 0.
- Finalize reads how each task ended from the Jobs API and gives core finalize the state of every
  unit that wrote no attempt file, so a lost, timed-out or cancelled task fails or cancels the
  run (a failed one is PLD011). If the API cannot be read, finalize prints one warning and judges
  by the attempt files alone. A one-time run has no job run id, so it gets no task states.

- Unity Catalog: `catalog: {kind: unity, catalog: main}` (Spark engine only) addresses tables and
  views as `` `catalog`.`schema`.`name` ``. The option catalog fills in when the target's naming
  leaves it empty. A schema is created only when it does not exist yet, so a principal without
  CREATE SCHEMA can write to an existing one.
- Secrets: `${secret:databricks/<scope>/<key>}` in plugin options reads a Databricks secret
  scope at run time, with the connection rules above. The value is masked like any resolved
  secret. A bad path, a missing connection or an API error is PLX109, naming the path and never
  the value.

License: Apache-2.0.
