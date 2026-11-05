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
- Deploying, running and repairing jobs arrive in later releases of this package.

License: Apache-2.0.
