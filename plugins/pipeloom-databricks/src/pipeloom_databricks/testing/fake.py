"""`FakeDatabricks`: an in-memory `DatabricksClient` for tests and case files.

No network and no SDK. Tests inspect `jobs`, `files`, `runs` and `calls`, and make a call fail with
`fail["create_job"] = ApiError(...)`. The scenario functions at the end are what a case file names
in `--client-factory pipeloom_databricks.testing.fake:<scenario>`.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import replace

from pydantic import JsonValue

from pipeloom_databricks.client import ApiError, DirEntry, JobSummary, RunInfo, TaskRunInfo

JOB_TAG = "pipeloom_job"


def _parent(path: str) -> str:
    return path.rsplit("/", 1)[0] if "/" in path.strip("/") else "/"


class FakeDatabricks:
    host = "https://fake.databricks.test"

    def __init__(self, *, user: str = "me@example.com") -> None:
        self.user = user
        self.jobs: dict[int, dict[str, JsonValue]] = {}
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = {"/"}
        self.modified: dict[str, int] = {}
        self.volume_files: dict[str, bytes] = {}
        self.secrets: dict[tuple[str, str], str] = {}
        """(scope, key) -> the plain value; `get_secret` returns it base64 encoded."""
        self.runs: dict[int, RunInfo] = {}
        self.run_tokens: dict[str, int] = {}
        self.repairs: list[tuple[int, tuple[str, ...], int | None]] = []
        self.starts: list[tuple[int, dict[str, str], tuple[str, ...] | None, str | None]] = []
        """Every `run_now`: (job_id, job_parameters, only, idempotency_token)."""
        self.submissions: list[tuple[dict[str, JsonValue], str | None]] = []
        """Every `submit_run`: (the spec, idempotency_token)."""
        self.calls: list[tuple[str, str]] = []
        """(method, first argument as text), in order."""
        self.fail: dict[str, ApiError] = {}
        self._next = 100
        self._tick = 0

    def _enter(self, method: str, arg: object = "") -> None:
        self.calls.append((method, str(arg)))
        if method in self.fail:
            raise self.fail[method]

    def _id(self) -> int:
        self._next += 1
        return self._next

    def _touch(self, path: str) -> None:
        self._tick += 1
        self.modified[path] = self._tick

    def called(self, method: str) -> list[str]:
        return [arg for name, arg in self.calls if name == method]

    # jobs

    def list_jobs(self, *, name: str | None = None) -> Iterator[JobSummary]:
        self._enter("list_jobs", name or "")
        for job_id, settings in sorted(self.jobs.items()):
            if name is not None and settings.get("name") != name:
                continue
            tags = settings.get("tags")
            tag_map = {str(k): str(v) for k, v in tags.items()} if isinstance(tags, dict) else {}
            yield JobSummary(job_id, str(settings.get("name", "")), tag_map)

    def add_job(self, settings: Mapping[str, JsonValue]) -> int:
        """Put a job in without a call record (test setup)."""
        job_id = self._id()
        self.jobs[job_id] = json.loads(json.dumps(dict(settings)))
        return job_id

    def create_job(self, settings: Mapping[str, JsonValue]) -> int:
        self._enter("create_job")
        return self.add_job(settings)

    def reset_job(self, job_id: int, settings: Mapping[str, JsonValue]) -> None:
        self._enter("reset_job", job_id)
        if job_id not in self.jobs:
            raise ApiError(404, f"Job {job_id} does not exist.", "RESOURCE_DOES_NOT_EXIST")
        self.jobs[job_id] = json.loads(json.dumps(dict(settings)))

    # runs

    def _start(self, job_id: int | None, params: Mapping[str, str], token: str | None) -> int:
        if token is not None and token in self.run_tokens:
            return self.run_tokens[token]
        run_id = self._id()
        self.runs[run_id] = RunInfo(run_id, job_id, "PENDING", None, dict(params))
        if token is not None:
            self.run_tokens[token] = run_id
        return run_id

    def run_now(
        self,
        job_id: int,
        *,
        job_parameters: Mapping[str, str],
        only: Sequence[str] | None,
        idempotency_token: str | None,
    ) -> int:
        self._enter("run_now", job_id)
        if job_id not in self.jobs:
            raise ApiError(404, f"Job {job_id} does not exist.", "RESOURCE_DOES_NOT_EXIST")
        self.starts.append(
            (
                job_id,
                dict(job_parameters),
                None if only is None else tuple(only),
                idempotency_token,
            )
        )
        return self._start(job_id, job_parameters, idempotency_token)

    def submit_run(self, spec: Mapping[str, JsonValue], *, idempotency_token: str | None) -> int:
        self._enter("submit_run")
        self.submissions.append((json.loads(json.dumps(dict(spec))), idempotency_token))
        return self._start(None, {}, idempotency_token)

    def set_run(
        self,
        run_id: int,
        *,
        job_id: int | None = None,
        life_cycle_state: str = "TERMINATED",
        result_state: str | None = None,
        tasks: Sequence[TaskRunInfo] = (),
        job_parameters: Mapping[str, str] | None = None,
        latest_repair_id: int | None = None,
        run_page_url: str | None = None,
    ) -> RunInfo:
        """Put a run in without a call record (test setup, or the simulator)."""
        run = RunInfo(
            run_id,
            job_id,
            life_cycle_state,
            result_state,
            dict(job_parameters or {}),
            tuple(tasks),
            latest_repair_id,
            run_page_url,
        )
        self.runs[run_id] = run
        return run

    def get_run(self, run_id: int) -> RunInfo:
        self._enter("get_run", run_id)
        if run_id not in self.runs:
            raise ApiError(404, f"Run {run_id} does not exist.", "RESOURCE_DOES_NOT_EXIST")
        return self.runs[run_id]

    def list_runs(self, job_id: int, *, active_only: bool = False) -> Iterator[RunInfo]:
        self._enter("list_runs", job_id)
        for run in self.runs.values():
            active = run.life_cycle_state not in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
            if run.job_id == job_id and (active or not active_only):
                yield run

    def repair_run(
        self, run_id: int, *, rerun_tasks: Sequence[str], latest_repair_id: int | None
    ) -> int:
        self._enter("repair_run", run_id)
        if run_id not in self.runs:
            raise ApiError(404, f"Run {run_id} does not exist.", "RESOURCE_DOES_NOT_EXIST")
        if latest_repair_id != self.runs[run_id].latest_repair_id:
            raise ApiError(
                400, "latest_repair_id is not the latest repair.", "INVALID_PARAMETER_VALUE"
            )
        repair_id = self._id()
        self.repairs.append((run_id, tuple(rerun_tasks), latest_repair_id))
        self.runs[run_id] = replace(
            self.runs[run_id], life_cycle_state="RUNNING", latest_repair_id=repair_id
        )
        return repair_id

    # workspace

    def current_user(self) -> str:
        self._enter("current_user")
        return self.user

    def mkdirs(self, path: str) -> None:
        self._enter("mkdirs", path)
        here = path.rstrip("/") or "/"
        while here not in self.dirs:
            self.dirs.add(here)  # like the real API, a folder has no modification time
            here = _parent(here)

    def upload(self, path: str, data: bytes) -> None:
        self._enter("upload", path)
        if _parent(path) not in self.dirs:
            raise ApiError(404, f"The parent of {path} does not exist.", "RESOURCE_DOES_NOT_EXIST")
        self.files[path] = data
        self._touch(path)

    def list_dir(self, path: str) -> list[DirEntry]:
        self._enter("list_dir", path)
        here = path.rstrip("/") or "/"
        if here not in self.dirs:
            raise ApiError(404, f"{path} does not exist.", "RESOURCE_DOES_NOT_EXIST")
        kids = [(p, True) for p in self.dirs if p != "/" and _parent(p) == here]
        kids += [(p, False) for p in self.files if _parent(p) == here]
        return [DirEntry(p, d, self.modified.get(p, 0)) for p, d in sorted(kids)]

    def delete(self, path: str, *, recursive: bool) -> None:
        self._enter("delete", path)
        here = path.rstrip("/")
        inside = [p for p in (*self.dirs, *self.files) if p.startswith(here + "/")]
        if here not in self.dirs and here not in self.files:
            raise ApiError(404, f"{path} does not exist.", "RESOURCE_DOES_NOT_EXIST")
        if inside and not recursive:
            raise ApiError(400, f"{path} is not empty.", "DIRECTORY_NOT_EMPTY")
        for p in inside:
            self.dirs.discard(p)
            self.files.pop(p, None)
        self.dirs.discard(here)
        self.files.pop(here, None)

    def read_file(self, path: str) -> bytes:
        self._enter("read_file", path)
        if path not in self.volume_files:
            raise ApiError(404, f"{path} does not exist.", "NOT_FOUND")
        return self.volume_files[path]

    def get_secret(self, scope: str, key: str) -> str:
        self._enter("get_secret", f"{scope}/{key}")
        if (scope, key) not in self.secrets:
            raise ApiError(404, f"Secret {scope}/{key} does not exist.", "RESOURCE_DOES_NOT_EXIST")
        return base64.b64encode(self.secrets[(scope, key)].encode("utf-8")).decode("ascii")


def empty() -> FakeDatabricks:
    """A workspace with nothing in it."""
    return FakeDatabricks()


def duplicate_jobs() -> FakeDatabricks:
    """Two jobs that both claim the job tag `shop.main.prod` (PLD006)."""
    client = FakeDatabricks()
    for _ in range(2):
        client.add_job({"name": "pipeloom_shop_main_prod", "tags": {JOB_TAG: "shop.main.prod"}})
    return client


def stale_job() -> FakeDatabricks:
    """A job for `shop.main.prod` deployed from other settings than any project renders (PLD008)."""
    client = FakeDatabricks()
    tags: dict[str, JsonValue] = {JOB_TAG: "shop.main.prod", "pipeloom_settings_hash": "0" * 64}
    client.add_job({"name": "pipeloom_shop_main_prod", "tags": tags})
    return client


def active_run() -> FakeDatabricks:
    """Job run 900 of job 101 is still running (PLD014)."""
    client = FakeDatabricks()
    job_id = client.add_job(
        {"name": "pipeloom_shop_main_prod", "tags": {JOB_TAG: "shop.main.prod"}}
    )
    client.set_run(900, job_id=job_id, life_cycle_state="RUNNING")
    return client


def api_down() -> FakeDatabricks:
    """A workspace whose job creation fails with a 500 (PLD009)."""
    client = FakeDatabricks()
    client.fail["create_job"] = ApiError(500, "INTERNAL_ERROR: the service is unavailable")
    return client
