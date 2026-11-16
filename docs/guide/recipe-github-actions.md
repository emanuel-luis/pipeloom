# Recipe: GitHub Actions

This workflow runs the TPC-H polars example through the [orchestrator protocol](orchestrators.md)
on a GitHub Actions runner. A shell loop plays the orchestrator: it reads the unit graph, runs each
unit with its own command line, finalizes the run and checks the result. Nothing in it is specific
to the example except the project folder and the count of nine units. The same file runs in this
repository's CI, so it stays correct.

```yaml
--8<-- ".github/workflows/orchestrate-tpch.yml"
```

## Step by step

1. **Install pipeloom.** The workflow installs this checkout with `uv sync --locked --all-packages`
   and puts `.venv/bin` on the path. This is the only step you replace in your own repository,
   with `pip install "pipeloom[polars]"` or whatever your project needs.
2. **Generate the data.** The example ships a script that writes the five TPC-H tables as parquet.
   Your pipeline reads its own sources, so you drop this step.
3. **Pick the run id and start time.** `gha-<run_id>-<attempt>` is a valid run id and changes when
   someone reruns the workflow. The start time is exported as `PIPELOOM_RUN_STARTED_AT`, so every
   later command sees the same one.
4. **Prepare.** `compile --stable` writes the manifest. `graph --units --output json --manifest`
   writes the unit graph with the plan hash of that manifest.
5. **Run the units.** The loop reads the units in topological order. A unit with a parent that
   failed or was skipped is skipped. For the others, `jq` fills the placeholders `{project_dir}`,
   `{manifest}` and `{run_id}` in the `argv` and the loop runs it. A failed unit is recorded and the
   step fails at the end, after every unit that could run has run.
6. **Finalize.** This step has `if: always()`, so a failed run still gets its `run_results.json`.
   It uses `finalize.argv` from the unit graph and adds the id of the workflow run with
   `--orchestrator-id`.
7. **Check.** `jq -e` fails the job unless the run succeeded with nine succeeded units.
8. **Upload.** The run folder, with the manifest, results, attempt files, logs and events, is
   attached to the workflow run even when the job failed.

The loop does not retry. In the example no model sets `retries`, and the protocol leaves retries to
the orchestrator: add a retry loop around a unit that reads `retries`, `retry_delay_s` and
`retry_on` from the unit graph and the `error.category` of the newest attempt file. Add a
`timeout` around the command for a unit with `timeout_s`.

## Several jobs

GitHub hosted runners share no filesystem, so one job per unit needs three things the example does
not have: a remote catalog, a remote state store, and an artifacts root on shared storage that
every job and the finalize job can read and write. With those in place each unit is a job that runs
its `argv`, a final job with `if: always()` runs finalize, and units that failed or were skipped are
passed to it with `--unit-states-json`. This variant is not tested in CI.

## Other orchestrators

Kestra, Argo Workflows and Airflow without the plugin follow the same protocol. A task runs a
unit's `argv`, task dependencies come from `edges`, and a last task runs finalize whatever happened
before it. The orchestrator's own retry and timeout settings come from the unit graph.
