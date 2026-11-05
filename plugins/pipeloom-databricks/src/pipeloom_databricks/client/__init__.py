"""The Databricks client the plugin talks through (ADR-0036).

A small JSON-shaped protocol with records of our own, so nothing outside `client/sdk.py` and
`client/connect.py` depends on the SDK. Importing this module loads no SDK.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from pydantic import JsonValue


@dataclass(frozen=True)
class JobSummary:
    job_id: int
    name: str
    tags: Mapping[str, str]


@dataclass(frozen=True)
class TaskRunInfo:
    task_key: str
    run_id: int
    attempt: int
    life_cycle_state: str
    result_state: str | None
    termination_code: str | None
    message: str | None


@dataclass(frozen=True)
class RunInfo:
    run_id: int
    job_id: int | None
    life_cycle_state: str
    result_state: str | None
    job_parameters: Mapping[str, str] = field(default_factory=dict)
    tasks: tuple[TaskRunInfo, ...] = ()
    latest_repair_id: int | None = None
    run_page_url: str | None = None
    start_time_ms: int | None = None
    repair_states: Mapping[int, str] = field(default_factory=dict)
    """Repair id -> life cycle state of that repair's entry in the run's repair history."""


@dataclass(frozen=True)
class DirEntry:
    path: str
    is_dir: bool
    modified_ms: int = 0
    """Last change in epoch milliseconds; 0 when the workspace does not say."""


class ApiError(Exception):
    """A failed API call. `status` is the HTTP status, 0 when the SDK did not say."""

    def __init__(self, status: int, message: str, error_code: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.error_code = error_code
        self.message = message


class ConnectionNotConfigured(Exception):
    """No workspace connection could be set up (PLD010). The message says why."""


class DatabricksClient(Protocol):
    """Every call the plugin makes. Failures raise `ApiError`."""

    host: str

    def list_jobs(self, *, name: str | None = None) -> Iterator[JobSummary]: ...
    def create_job(self, settings: Mapping[str, JsonValue]) -> int: ...
    def reset_job(self, job_id: int, settings: Mapping[str, JsonValue]) -> None: ...
    def run_now(
        self,
        job_id: int,
        *,
        job_parameters: Mapping[str, str],
        only: Sequence[str] | None,
        idempotency_token: str | None,
    ) -> int: ...
    def submit_run(
        self, spec: Mapping[str, JsonValue], *, idempotency_token: str | None
    ) -> int: ...
    def get_run(self, run_id: int) -> RunInfo: ...
    def list_runs(self, job_id: int, *, active_only: bool = False) -> Iterator[RunInfo]: ...
    def repair_run(
        self,
        run_id: int,
        *,
        rerun_tasks: Sequence[str],
        latest_repair_id: int | None,
        job_parameters: Mapping[str, str] | None = None,
    ) -> int: ...
    def current_user(self) -> str: ...
    def mkdirs(self, path: str) -> None: ...
    def upload(self, path: str, data: bytes) -> None:
        """Store `data` as a plain workspace file, replacing what is there."""
        ...

    def list_dir(self, path: str) -> list[DirEntry]: ...
    def delete(self, path: str, *, recursive: bool) -> None: ...
    def read_file(self, path: str) -> bytes:
        """The bytes of a file on a volume (Files API)."""
        ...

    def get_secret(self, scope: str, key: str) -> str:
        """The `value` the API returns: the secret, base64 encoded."""
        ...


def connect(
    *, profile: str | None, env: Mapping[str, str], factory: str | None = None
) -> DatabricksClient:
    """A client for the workspace the arguments and `env` select; see `client.connect`.

    Raises `ConnectionNotConfigured` when nothing is configured.
    """
    from pipeloom_databricks.client.connect import connect as _connect

    return _connect(profile=profile, env=env, factory=factory)
