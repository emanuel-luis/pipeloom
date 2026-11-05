# ADR-0036: Databricks client and connection

## Context

Deploying a job needs a workspace: list and create jobs, upload the project, read the current user. Later releases of the plugin also run and repair jobs and read secrets. The plugin must be testable without a workspace, must never use a credential the user did not choose, and must not break when the installed SDK lags the API.

Facts checked against public documentation. The Jobs API 2.2 paths used are `jobs/list`, `jobs/create`, `jobs/reset`, `jobs/run-now`, `jobs/runs/submit`, `jobs/runs/get` (with `include_history`, paginated by `next_page_token` over `tasks`), `jobs/runs/list` and `jobs/runs/repair`. Workspace import (`POST /api/2.0/workspace/import`) takes base64 `content`, a 10 MB limit and `overwrite`. Its `format` values are `SOURCE`, `HTML`, `JUPYTER`, `DBC`, `R_MARKDOWN`, `AUTO` and `RAW`; only `AUTO` is described in prose ("imported depending on an analysis of the item's extension and the header content"), and `RAW` has no description anywhere in the REST reference, the SDK reference or the CLI reference. Sources: https://docs.databricks.com/api/workspace/workspace/import and https://docs.databricks.com/aws/en/dev-tools/cli/reference/workspace-commands.

## Options

- **The typed SDK methods** (`w.jobs.create(...)`). They lag the API and drop fields such as `environment_version`. Rejected.
- **A hand-written HTTP client.** Would re-implement authentication, retries and error mapping. Rejected.
- **The SDK's `api_client.do`.** Authentication, retries and errors come from the SDK, and the JSON is ours, unchanged.
- **Workspace import format `RAW`.** Probably stores a plain file, but nothing public says so. Rejected until documented.
- **Picking the `DEFAULT` profile when nothing is set.** Convenient, and a way to deploy to a workspace the user did not mean. Rejected.

## Decision

The plugin owns a small `DatabricksClient` protocol with JSON-shaped records (`JobSummary`, `RunInfo`, `TaskRunInfo`, `DirEntry`) and one error type, `ApiError` (HTTP status, error code, message). `SdkClient` implements it with `WorkspaceClient.api_client.do` for every call; the endpoints are the ones listed above plus `GET /api/2.0/preview/scim/v2/Me`, the workspace `mkdirs`, `list` and `delete`, `GET /api/2.0/fs/files<path>` (volumes) and `GET /api/2.0/secrets/get`. A task run's state is read from `status` when present and from the legacy `state` otherwise; a termination code that is not a result state name counts as `FAILED`. `get_secret` returns the base64 `value` exactly as the API does. SDK errors become `ApiError`, with the message passed through the redactor of the connection, which registers the token and client secret it was built with.

Workspace files are stored with import format `AUTO`: it is the only documented format that does not turn the content into a notebook unless the file has a notebook header or a notebook extension. The upload set therefore refuses such files (PLD012): a Databricks notebook header (`# Databricks notebook source`, `-- ...`, `// ...`), an `.ipynb` file, a file above 10 MB, or one outside the project folder. If `RAW` is documented as a plain-file format, switching is one constant (`IMPORT_FORMAT`).

`databricks.sdk` is imported lazily and only in `client/sdk.py`, `client/connect.py` and `task_values.py`; import-linter enforces it. The dependency is bounded `databricks-sdk>=0.50,<1`, and the SDK client tests run against 0.50.0 as well as the locked version.

The connection is chosen in this order, and never by guessing: `--profile`; a profile the user set in `DATABRICKS_CONFIG_PROFILE`; `DATABRICKS_HOST` with `DATABRICKS_TOKEN` or `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`; runtime authentication when `DATABRICKS_RUNTIME_VERSION` is set; otherwise PLD010. Without a profile the SDK gets a `config_file` in a fresh temporary folder where no file exists, so it cannot fall back to a `DEFAULT` profile in `~/.databrickscfg`. Host and credentials come from the mapping the caller passes, not from ambient state. A hidden group option `--client-factory MODULE:ATTR` replaces all of this with a zero-argument callable that returns a client; tests and case files use it.

`pipeloom_databricks.testing.fake.FakeDatabricks` is an in-memory client: jobs, a workspace tree with modification ticks, runs, secrets, a call log and injectable failures. Tests of the plugin run with every `DATABRICKS_*` variable removed, `DATABRICKS_CONFIG_FILE` pointing at nothing and `databricks.sdk.WorkspaceClient`, `databricks.sdk.core.Config` and `databricks.sdk.config.Config` replaced by functions that fail, so a test that builds a real client fails. Only the connect tests put recorders in their place.

Direct deploy (`pipeloom databricks deploy`) uploads the project to `<workspace_root>/<project>/<pipeline>/<target>/<hash[:12]>`, where the hash covers every uploaded path and its bytes and the stable manifest. The default `workspace_root` is `/Workspace/Users/<current user>/.pipeloom`. A folder holding the marker `_pipeloom/complete`, written last, is never uploaded again. The job's parameters name that folder, so a changed project changes the settings hash; equal hashes leave the job alone, otherwise the job is created (found by its `pipeloom_job` tag, by name first and by a scan of all jobs second; two matches are PLD006) or reset. The upload set is `pipeloom.yml`, the model and source files of the plan, the `*.py` files below `python_paths` (skipping dot entries, `__pycache__`, `venv`, `.venv`, `dist`, `build`, `node_modules`, folders with `pyvenv.cfg` and the artifacts root) and the stable manifest, so data files, wheels and credentials stay local. The workspace API gives folders no modification time, so folders are ordered by the modification time of their marker file, which every deploy that points the job at a folder writes again. The current folder and the two newest others are kept (the newest other is the folder the job used before), older ones are deleted, a folder that cannot be inspected is never deleted, and a failed delete is a warning. API failures are PLD009. `--dry-run` prints the settings and creates no client.

## Consequences

Fields the SDK does not know yet reach the API intact, and the plugin does not depend on SDK dataclasses. A deploy never reaches a workspace the user did not name. The protocol is small enough to fake completely, so deploy, run and repair are tested without a network. `list_dir` returns `DirEntry` with a modification time (the blueprint's tuple has none); it is meaningful for files only, which is why pruning reads the marker file.

Unconfirmed until the manual workflow runs against a workspace: that `AUTO` keeps a plain `.py` or `.yml` as a file, that the workspace API accepts a path with the `/Workspace` prefix, that tasks can read the uploaded folder through that prefix, and the mapping from `termination_details.code` to a result state. The job `reset` overwrites edits made in the UI; use the `settings` passthrough instead. Revisit `RAW` when it is documented.

## Status

Accepted.

## Date

2026-11-05
