"""Live checks of pipeloom-databricks against a real workspace.

Run only by the manually dispatched workflow `.github/workflows/databricks-live.yml`, never by
regular CI and never from a developer machine with a personal profile. It deploys the project in
`tests/fixtures/databricks_live`, runs it, repairs it and prints one line per fact:

    CHECK <fact> ok|failed

The facts are the ones ADR-0035 and ADR-0036 list as unconfirmed until a live run. The check
functions are pure (they read run records and run_results documents); the Databricks calls go
through the plugin client, the `pipeloom databricks` commands and a few injected callables, so
`tests/test_databricks_live.py` runs the whole flow against `FakeDatabricks`.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from pipeloom_databricks.client import ApiError, DatabricksClient, RunInfo

ROOT = Path(__file__).resolve().parent.parent
FIXTURE = ROOT / "tests" / "fixtures" / "databricks_live"
TARGET = "live"
WHEELS = ("pipeloom", "pipeloom_spark", "pipeloom_databricks")
"""Wheel file name prefixes, in the order of the fixture's `wheel_*` vars."""

Cli = Callable[[Sequence[str]], tuple[int, str]]
"""Runs `pipeloom databricks <args>`; returns the exit code and stdout."""


@dataclass(frozen=True)
class Check:
    fact: str
    ok: bool
    detail: str = ""

    def line(self) -> str:
        return f"CHECK {self.fact} {'ok' if self.ok else 'failed'}"


@dataclass(frozen=True)
class Cleanup:
    """What the run leaves behind and how to remove it; each callable may raise."""

    delete_job: Callable[[int], None]
    drop_schema: Callable[[], None]
    delete_folder: Callable[[str], None]


def last_json(stdout: str) -> dict[str, Any]:
    """The last stdout line that is a JSON object, or an empty dict."""
    for text in reversed(stdout.strip().splitlines()):
        try:
            doc = json.loads(text)
        except ValueError:
            continue
        if isinstance(doc, dict):
            return doc
    return {}


def json_lines(stdout: str) -> list[dict[str, Any]]:
    out = []
    for text in stdout.strip().splitlines():
        try:
            doc = json.loads(text)
        except ValueError:
            continue
        if isinstance(doc, dict):
            out.append(doc)
    return out


def unit_rows(doc: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["unit_id"]: row for row in doc.get("results", [])}


def read_results(client: DatabricksClient, artifacts_root: str, run_id: str) -> dict[str, Any]:
    path = f"{artifacts_root.rstrip('/')}/runs/{run_id}/run_results.json"
    return json.loads(client.read_file(path).decode("utf-8"))  # type: ignore[no-any-return]


def task_result(run: RunInfo, key: str) -> str | None:
    return next((t.result_state for t in run.tasks if t.task_key == key), None)


def _ids_resolved(row: Mapping[str, Any], job_run_id: int) -> bool:
    ids = row.get("orchestrator_ids") or {}
    return str(ids.get("databricks_job_run_id")) == str(job_run_id) and not any(
        "{{" in str(v) for v in ids.values()
    )


def check_full_run(run: RunInfo, doc: Mapping[str, Any], exit_code: int) -> list[Check]:
    """A run without `--run-id` over the stable pipeline (models a and b)."""
    rows = unit_rows(doc)
    both = {"a", "b"} <= rows.keys() and all(rows[u]["status"] == "succeeded" for u in "ab")
    attempts = both and all(rows[u]["attempt"] == 1 for u in "ab")
    return [
        Check("run_succeeded", exit_code == 0 and run.result_state == "SUCCESS"),
        Check(
            "empty_parameter_default_resolves_run_id",
            doc.get("run", {}).get("run_id") == f"dbx-{run.run_id}",
        ),
        Check(
            "dynamic_references_in_wheel_parameters",
            both and all(_ids_resolved(rows[u], run.run_id) for u in "ab"),
        ),
        Check("spark_session_in_wheel_task", both),
        Check("attempt_files_on_volume", attempts),
        Check(
            "job_start_time_reference",
            run.start_time_ms is not None and bool(doc.get("run", {}).get("started_at")),
        ),
    ]


def check_select(run: RunInfo, doc: Mapping[str, Any], exit_code: int) -> list[Check]:
    """`--select a` with an ALL_DONE finalize whose dependencies are excluded."""
    rows = unit_rows(doc)
    return [
        Check(
            "only_with_all_done_finalize",
            exit_code == 0
            and run.result_state == "SUCCESS"
            and task_result(run, "pipeloom__finalize") == "SUCCESS"
            and task_result(run, "b") == "EXCLUDED"
            and "b" not in rows
            and rows.get("a", {}).get("status") == "succeeded",
        )
    ]


def check_repair(
    failed_run: RunInfo,
    failed_exit: int,
    before: Mapping[str, Any],
    repaired: RunInfo,
    repair_exit: int,
    after: Mapping[str, Any],
) -> list[Check]:
    first = unit_rows(before).get("c", {})
    second = unit_rows(after).get("c", {})
    return [
        Check(
            "failed_attempt_one",
            failed_exit == 1 and failed_run.result_state != "SUCCESS" and first.get("attempt") == 1,
        ),
        Check(
            "repair_runs_attempt_two",
            repair_exit == 0
            and repaired.result_state == "SUCCESS"
            and second.get("status") == "succeeded"
            and second.get("attempt") == 2
            and after.get("run", {}).get("status") == "succeeded",
        ),
        Check(
            "job_start_time_across_repairs",
            bool(before.get("run", {}).get("started_at"))
            and before["run"]["started_at"] == after.get("run", {}).get("started_at"),
        ),
    ]


def _vars(
    *, catalog: str, schema: str, volume: str, environment_version: str, wheels: Sequence[str]
) -> str:
    doc = {
        "catalog": catalog,
        "schema": schema,
        "volume": volume,
        "environment_version": environment_version,
        **dict(zip(("wheel_core", "wheel_spark", "wheel_databricks"), wheels, strict=True)),
    }
    return json.dumps(doc)


def wheel_paths(dist: Path, volume: str, sha: str) -> dict[str, str]:
    """Wheel file name -> its path on the volume, for the wheels found in `dist`."""
    base = f"{volume.rstrip('/')}/wheels/{sha}"
    return {w.name: f"{base}/{w.name}" for w in sorted(dist.glob("*.whl"))}


def pick_wheels(paths: Mapping[str, str]) -> list[str]:
    """The three wheels the fixture installs, in `WHEELS` order."""
    out = []
    for prefix in WHEELS:
        hits = [p for name, p in paths.items() if name.startswith(prefix + "-")]
        if len(hits) != 1:
            raise SystemExit(f"expected one wheel for {prefix}, found {len(hits)}")
        out.append(hits[0])
    return out


def run_checks(
    client: DatabricksClient,
    cli: Cli,
    artifacts_root: str,
    tag: str,
    say: Callable[[str], None],
    job_ids: list[int],
    folders: list[str],
) -> list[Check]:
    """Deploy, run, select, fail and repair. Fills `job_ids` and `folders` as it goes, so a
    failure half way still leaves them for the cleanup."""
    checks: list[Check] = []
    code, out = cli(["deploy"])
    jobs = {d["pipeline"]: d for d in json_lines(out) if "job_id" in d}
    job_ids += [int(d["job_id"]) for d in jobs.values()]
    folders += [str(d["root"]) for d in jobs.values()]
    checks.append(Check("deploy", code == 0 and {"stable", "flaky"} <= jobs.keys(), out))
    if not checks[-1].ok:
        return checks

    def start(*args: str) -> tuple[int, dict[str, Any], str]:
        code, out = cli(["run", *args])
        say(f"run {' '.join(args)} -> exit {code}")
        started = last_json(out)
        run_id = started.get("run_id") or f"dbx-{started.get('job_run_id')}"
        return code, started, str(run_id)

    code, started, run_id = start("--pipeline", "stable")
    if "job_run_id" in started:
        run = client.get_run(int(started["job_run_id"]))
        checks += check_full_run(run, read_results(client, artifacts_root, run_id), code)
    else:
        checks.append(Check("run_succeeded", False, "no run was started"))

    select_id = f"live-select-{tag}"
    code, started, run_id = start("--pipeline", "stable", "--select", "a", "--run-id", select_id)
    if "job_run_id" in started:
        run = client.get_run(int(started["job_run_id"]))
        checks += check_select(run, read_results(client, artifacts_root, run_id), code)
    else:
        checks.append(Check("only_with_all_done_finalize", False, "no run was started"))

    flaky_id = f"live-flaky-{tag}"
    code, started, run_id = start("--pipeline", "flaky", "--run-id", flaky_id)
    if "job_run_id" not in started:
        checks.append(Check("failed_attempt_one", False, "no run was started"))
        return checks
    failed = client.get_run(int(started["job_run_id"]))
    before = read_results(client, artifacts_root, run_id)
    repair_code, _ = cli(["repair", "--pipeline", "flaky", "--run-id", flaky_id])
    repaired = client.get_run(failed.run_id)
    after = read_results(client, artifacts_root, run_id)
    checks += check_repair(failed, code, before, repaired, repair_code, after)
    return checks


def cleanup(
    job_ids: Sequence[int], folders: Sequence[str], hooks: Cleanup, say: Callable[[str], None]
) -> None:
    """Delete the jobs, the upload folders and the schema; a failure is reported, not raised."""
    steps: list[tuple[str, Callable[[], None]]] = [
        *((f"job {j}", partial(hooks.delete_job, j)) for j in job_ids),
        *((f"folder {f}", partial(hooks.delete_folder, f)) for f in folders),
        ("schema", hooks.drop_schema),
    ]
    for label, step in steps:
        try:
            step()
        except Exception as err:  # noqa: BLE001 - cleanup must try every step
            say(f"cleanup of {label} failed: {err}")


def execute(
    client: DatabricksClient,
    cli: Cli,
    hooks: Cleanup,
    *,
    artifacts_root: str,
    keep_job: bool,
    tag: str,
    say: Callable[[str], None],
) -> int:
    """The whole flow; prints the CHECK lines and returns the exit code (1 when any failed)."""
    job_ids: list[int] = []
    folders: list[str] = []
    checks: list[Check] = []
    try:
        checks = run_checks(client, cli, artifacts_root, tag, say, job_ids, folders)
    except ApiError as err:
        checks.append(Check("workspace_api", False, err.message))
    finally:
        if not keep_job:
            cleanup(job_ids, folders, hooks, say)
    for check in checks:
        print(check.line())
        if not check.ok and check.detail:
            say(f"{check.fact}: {check.detail}")
    return 0 if checks and all(c.ok for c in checks) else 1


def _subprocess_cli(project: Path, vars_json: str, env: Mapping[str, str]) -> Cli:
    def run(args: Sequence[str]) -> tuple[int, str]:
        base = [sys.executable, "-m", "pipeloom", "databricks"]
        # group options first, then the command and its own options
        cmd = [*base, args[0], "--project-dir", str(project), "--target", TARGET]
        cmd += ["--vars", vars_json, *args[1:]]
        done = subprocess.run(cmd, capture_output=True, text=True, env=dict(env), check=False)
        sys.stderr.write(done.stderr)
        return done.returncode, done.stdout

    return run


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--catalog", required=True)
    parser.add_argument(
        "--volume", required=True, help="Volume path, /Volumes/<catalog>/<schema>/<name>"
    )
    parser.add_argument("--environment-version", default="5")
    parser.add_argument("--keep-job", action="store_true")
    parser.add_argument("--dist", type=Path, default=ROOT / "dist")
    parser.add_argument(
        "--sha", required=True, help="Commit sha: the wheels go to <volume>/wheels/<sha>/"
    )
    args = parser.parse_args(argv)

    from databricks.sdk import WorkspaceClient  # only on the workflow runner

    from pipeloom_databricks.client import connect

    # the workflow maps every secret, so the ones that are not configured arrive as empty strings
    for key in [k for k, v in os.environ.items() if k.startswith("DATABRICKS_") and not v]:
        del os.environ[key]
    env = os.environ
    client = connect(profile=None, env=env)
    sdk = WorkspaceClient(config_file=env.get("DATABRICKS_CONFIG_FILE"))
    tag = uuid.uuid4().hex[:8]
    schema = f"pipeloom_live_{tag}"
    paths = wheel_paths(args.dist, args.volume, args.sha)
    for name, target in paths.items():
        with (args.dist / name).open("rb") as handle:
            sdk.files.upload(target, handle, overwrite=True)
    vars_json = _vars(
        catalog=args.catalog,
        schema=schema,
        volume=args.volume,
        environment_version=args.environment_version,
        wheels=pick_wheels(paths),
    )
    hooks = Cleanup(
        delete_job=lambda job_id: sdk.jobs.delete(job_id),
        drop_schema=lambda: sdk.schemas.delete(f"{args.catalog}.{schema}", force=True),
        delete_folder=lambda path: client.delete(path, recursive=True),
    )
    return execute(
        client,
        _subprocess_cli(FIXTURE, vars_json, env),
        hooks,
        artifacts_root=f"{args.volume.rstrip('/')}/artifacts",
        keep_job=args.keep_job,
        tag=tag,
        say=lambda line: print(line, file=sys.stderr),
    )


if __name__ == "__main__":
    raise SystemExit(main())
