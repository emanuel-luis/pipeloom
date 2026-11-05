"""`SdkClient`: the REST calls of the plugin, every one through `WorkspaceClient.api_client.do`.

Going through `do` (not the typed SDK methods) keeps the JSON exactly as rendered, so a field the
installed SDK does not know yet, such as `environment_version`, is never dropped. This is one of
the three modules that import `databricks.sdk`; `connect` imports this one lazily.
"""

from __future__ import annotations

import base64
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Final, Protocol
from urllib.parse import quote

from databricks.sdk.errors.base import DatabricksError
from pydantic import JsonValue

from pipeloom.secrets.redaction import Redactor
from pipeloom_databricks.client import ApiError, DirEntry, JobSummary, RunInfo, TaskRunInfo

JOBS: Final = "/api/2.2/jobs"
WORKSPACE: Final = "/api/2.0/workspace"
IMPORT_FORMAT: Final = "AUTO"
"""The workspace import format; ADR-0036 explains why."""
_STATUS: Final = {
    "BadRequest": 400,
    "Unauthenticated": 401,
    "PermissionDenied": 403,
    "NotFound": 404,
    "Conflict": 409,
    "TooManyRequests": 429,
    "InternalError": 500,
    "TemporarilyUnavailable": 503,
}
_RESULT: Final = {
    "SUCCESS": "SUCCESS",
    "SUCCESS_WITH_FAILURES": "SUCCESS_WITH_FAILURES",
    "USER_CANCELED": "CANCELED",
    "CANCELED": "CANCELED",
    "UPSTREAM_FAILED": "UPSTREAM_FAILED",
    "UPSTREAM_CANCELED": "UPSTREAM_CANCELED",
    "MAX_CONCURRENT_RUNS_EXCEEDED": "MAXIMUM_CONCURRENT_RUNS_REACHED",
    "EXCLUDED": "EXCLUDED",
    "DISABLED": "DISABLED",
}
"""`termination_details.code` values that name a result state; any other code is a failure."""


class _Api(Protocol):
    def do(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = ...,
        body: dict[str, Any] | None = ...,
        raw: bool = ...,
    ) -> Any: ...


def _status(err: DatabricksError) -> int:
    for cls in type(err).__mro__:
        if cls.__name__ in _STATUS:
            return _STATUS[cls.__name__]
    return 0


def _states(item: Mapping[str, Any]) -> tuple[str, str | None, str | None, str | None]:
    """life cycle state, result state, termination code and message of a run or a task run.

    The `status` object is read when present, the legacy `state` object otherwise.
    """
    state: Mapping[str, Any] = item.get("state") or {}
    status: Mapping[str, Any] = item.get("status") or {}
    details: Mapping[str, Any] = status.get("termination_details") or {}
    life = (status.get("state") if status else None) or state.get("life_cycle_state") or "UNKNOWN"
    code = details.get("code")
    result = state.get("result_state")
    if result is None and life == "TERMINATED" and code:
        result = _RESULT.get(code, "FAILED")
    message = details.get("message") or state.get("state_message") or status.get("state_message")
    return str(life), result, code, message or None


def _task(raw: Mapping[str, Any]) -> TaskRunInfo:
    life, result, code, message = _states(raw)
    return TaskRunInfo(
        task_key=str(raw["task_key"]),
        run_id=int(raw["run_id"]),
        attempt=int(raw.get("attempt_number") or 0),
        life_cycle_state=life,
        result_state=result,
        termination_code=code,
        message=message,
    )


def _run(raw: Mapping[str, Any], tasks: Sequence[Mapping[str, Any]] = ()) -> RunInfo:
    life, result, _, _ = _states(raw)
    params: dict[str, str] = {}
    for p in raw.get("job_parameters") or []:
        value = p.get("value", p.get("default"))
        if value is not None:
            params[str(p["name"])] = str(value)
    repairs = [int(h["id"]) for h in raw.get("repair_history") or [] if h.get("id") is not None]
    job_id = raw.get("job_id")
    start = raw.get("start_time")
    return RunInfo(
        run_id=int(raw["run_id"]),
        job_id=None if job_id is None else int(job_id),
        life_cycle_state=life,
        result_state=result,
        job_parameters=params,
        tasks=tuple(_task(t) for t in tasks),
        latest_repair_id=max(repairs) if repairs else None,
        run_page_url=raw.get("run_page_url"),
        start_time_ms=None if start is None else int(start),
    )


class SdkClient:
    """A `DatabricksClient` over the `api_client` of a `WorkspaceClient` (or a fake of it)."""

    def __init__(self, api_client: _Api, host: str, redactor: Redactor | None = None) -> None:
        self._api = api_client
        self.host = host
        self._redactor = redactor

    def _call(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        raw: bool = False,
    ) -> Any:
        try:
            return self._api.do(method, path, query=query, body=body, raw=raw)
        except DatabricksError as err:
            text = str(err)
            if self._redactor is not None:
                text = self._redactor.mask(text)
            raise ApiError(_status(err), text, err.error_code) from None

    def list_jobs(self, *, name: str | None = None) -> Iterator[JobSummary]:
        token: str | None = None
        while True:
            query: dict[str, Any] = {"limit": 100}
            if name is not None:
                query["name"] = name
            if token:
                query["page_token"] = token
            page = self._call("GET", f"{JOBS}/list", query=query) or {}
            for job in page.get("jobs") or []:
                settings = job.get("settings") or {}
                tags = {str(k): str(v) for k, v in (settings.get("tags") or {}).items()}
                yield JobSummary(int(job["job_id"]), str(settings.get("name", "")), tags)
            token = page.get("next_page_token")
            if not token:
                return

    def create_job(self, settings: Mapping[str, JsonValue]) -> int:
        out = self._call("POST", f"{JOBS}/create", body=dict(settings))
        return int(out["job_id"])

    def reset_job(self, job_id: int, settings: Mapping[str, JsonValue]) -> None:
        self._call("POST", f"{JOBS}/reset", body={"job_id": job_id, "new_settings": dict(settings)})

    def run_now(
        self,
        job_id: int,
        *,
        job_parameters: Mapping[str, str],
        only: Sequence[str] | None,
        idempotency_token: str | None,
    ) -> int:
        body: dict[str, Any] = {"job_id": job_id, "job_parameters": dict(job_parameters)}
        if only is not None:
            body["only"] = list(only)
        if idempotency_token is not None:
            body["idempotency_token"] = idempotency_token
        return int(self._call("POST", f"{JOBS}/run-now", body=body)["run_id"])

    def submit_run(self, spec: Mapping[str, JsonValue], *, idempotency_token: str | None) -> int:
        body: dict[str, Any] = dict(spec)
        if idempotency_token is not None:
            body["idempotency_token"] = idempotency_token
        return int(self._call("POST", f"{JOBS}/runs/submit", body=body)["run_id"])

    def get_run(self, run_id: int) -> RunInfo:
        query: dict[str, Any] = {"run_id": run_id, "include_history": "true"}
        first = self._call("GET", f"{JOBS}/runs/get", query=query) or {}
        tasks: list[Mapping[str, Any]] = list(first.get("tasks") or [])
        token = first.get("next_page_token")
        while token:
            page = self._call("GET", f"{JOBS}/runs/get", query={**query, "page_token": token}) or {}
            tasks.extend(page.get("tasks") or [])
            token = page.get("next_page_token")
        return _run(first, tasks)

    def list_runs(self, job_id: int, *, active_only: bool = False) -> Iterator[RunInfo]:
        token: str | None = None
        while True:
            query: dict[str, Any] = {"job_id": job_id, "limit": 25}
            if active_only:
                query["active_only"] = "true"
            if token:
                query["page_token"] = token
            page = self._call("GET", f"{JOBS}/runs/list", query=query) or {}
            for run in page.get("runs") or []:
                yield _run(run, run.get("tasks") or [])
            token = page.get("next_page_token")
            if not token:
                return

    def repair_run(
        self, run_id: int, *, rerun_tasks: Sequence[str], latest_repair_id: int | None
    ) -> int:
        body: dict[str, Any] = {
            "run_id": run_id,
            "rerun_tasks": list(rerun_tasks),
            "rerun_dependent_tasks": True,
        }
        if latest_repair_id is not None:
            body["latest_repair_id"] = latest_repair_id
        return int(self._call("POST", f"{JOBS}/runs/repair", body=body)["repair_id"])

    def current_user(self) -> str:
        out = self._call("GET", "/api/2.0/preview/scim/v2/Me")
        return str(out["userName"])

    def mkdirs(self, path: str) -> None:
        self._call("POST", f"{WORKSPACE}/mkdirs", body={"path": path})

    def upload(self, path: str, data: bytes) -> None:
        body = {
            "path": path,
            "format": IMPORT_FORMAT,
            "content": base64.b64encode(data).decode("ascii"),
            "overwrite": True,
        }
        self._call("POST", f"{WORKSPACE}/import", body=body)

    def list_dir(self, path: str) -> list[DirEntry]:
        out = self._call("GET", f"{WORKSPACE}/list", query={"path": path}) or {}
        return [
            DirEntry(
                str(o["path"]), o.get("object_type") == "DIRECTORY", int(o.get("modified_at") or 0)
            )
            for o in out.get("objects") or []
        ]

    def delete(self, path: str, *, recursive: bool) -> None:
        self._call("POST", f"{WORKSPACE}/delete", body={"path": path, "recursive": recursive})

    def read_file(self, path: str) -> bytes:
        out = self._call("GET", f"/api/2.0/fs/files{quote(path, safe='/')}", raw=True)
        stream = out["contents"]
        try:
            data = stream.read()
        finally:
            close = getattr(stream, "close", None)
            if close is not None:
                close()
        return bytes(data)

    def get_secret(self, scope: str, key: str) -> str:
        out = self._call("GET", "/api/2.0/secrets/get", query={"scope": scope, "key": key})
        return str(out["value"])
