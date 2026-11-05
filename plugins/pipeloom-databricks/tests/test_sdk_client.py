"""`SdkClient` against a fake `api_client`: method, path, query and body are exact."""

from __future__ import annotations

import base64
import io
from typing import Any

import pytest
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.errors.platform import NotFound, PermissionDenied

from pipeloom.secrets.redaction import Redactor
from pipeloom_databricks.client import ApiError, DirEntry, JobSummary
from pipeloom_databricks.client.sdk import SdkClient


class FakeApi:
    """Answers `do` from a queue and records what it was asked."""

    def __init__(self, *answers: Any) -> None:
        self.answers = list(answers)
        self.calls: list[dict[str, Any]] = []

    def do(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        raw: bool = False,
    ) -> Any:
        self.calls.append(
            {"method": method, "path": path, "query": query, "body": body, "raw": raw}
        )
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def client(*answers: Any, redactor: Redactor | None = None) -> tuple[SdkClient, FakeApi]:
    api = FakeApi(*answers)
    return SdkClient(api, "https://h.example", redactor), api


def test_list_jobs_pages_and_filters_by_name() -> None:
    sdk, api = client(
        {
            "jobs": [{"job_id": 1, "settings": {"name": "a", "tags": {"k": "v"}}}],
            "next_page_token": "t2",
        },
        {"jobs": [{"job_id": 2, "settings": {"name": "a"}}]},
    )
    assert list(sdk.list_jobs(name="a")) == [JobSummary(1, "a", {"k": "v"}), JobSummary(2, "a", {})]
    assert api.calls[0] == {
        "method": "GET",
        "path": "/api/2.2/jobs/list",
        "query": {"limit": 100, "name": "a"},
        "body": None,
        "raw": False,
    }
    assert api.calls[1]["query"] == {"limit": 100, "name": "a", "page_token": "t2"}


def test_list_jobs_without_name_and_empty_workspace() -> None:
    sdk, api = client({})
    assert list(sdk.list_jobs()) == []
    assert api.calls[0]["query"] == {"limit": 100}


def test_create_and_reset_send_the_settings_unchanged() -> None:
    settings = {
        "name": "j",
        "environments": [{"environment_key": "e", "spec": {"environment_version": "5"}}],
    }
    sdk, api = client({"job_id": 7}, {})
    assert sdk.create_job(settings) == 7
    sdk.reset_job(7, settings)
    assert api.calls[0]["method"] == "POST" and api.calls[0]["path"] == "/api/2.2/jobs/create"
    assert api.calls[0]["body"] == settings
    assert api.calls[1]["path"] == "/api/2.2/jobs/reset"
    assert api.calls[1]["body"] == {"job_id": 7, "new_settings": settings}


def test_run_now_body() -> None:
    sdk, api = client({"run_id": 9}, {"run_id": 10})
    assert sdk.run_now(5, job_parameters={"a": "b"}, only=["t1"], idempotency_token="tok") == 9
    assert api.calls[0]["path"] == "/api/2.2/jobs/run-now"
    assert api.calls[0]["body"] == {
        "job_id": 5,
        "job_parameters": {"a": "b"},
        "only": ["t1"],
        "idempotency_token": "tok",
    }
    sdk.run_now(5, job_parameters={}, only=None, idempotency_token=None)
    assert api.calls[1]["body"] == {"job_id": 5, "job_parameters": {}}


def test_submit_run_body() -> None:
    sdk, api = client({"run_id": 3}, {"run_id": 4})
    assert sdk.submit_run({"run_name": "r", "tasks": []}, idempotency_token="t") == 3
    assert api.calls[0]["path"] == "/api/2.2/jobs/runs/submit"
    assert api.calls[0]["body"] == {"run_name": "r", "tasks": [], "idempotency_token": "t"}
    sdk.submit_run({"run_name": "r"}, idempotency_token=None)
    assert api.calls[1]["body"] == {"run_name": "r"}


RUN = {
    "run_id": 11,
    "job_id": 5,
    "state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"},
    "job_parameters": [
        {"name": "pipeloom_run_id", "default": "", "value": "r1"},
        {"name": "full_refresh", "default": "false"},
    ],
    "run_page_url": "https://h.example/run/11",
    "start_time": 1700000000000,
    "repair_history": [
        {"type": "ORIGINAL"},
        {"type": "REPAIR", "id": 21},
        {"type": "REPAIR", "id": 22},
    ],
    "tasks": [
        {
            "task_key": "a",
            "run_id": 12,
            "attempt_number": 1,
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": "boom",
            },
        }
    ],
}


def test_get_run_merges_task_pages_and_reads_history() -> None:
    page2 = {
        "tasks": [
            {
                "task_key": "b",
                "run_id": 13,
                "status": {
                    "state": "TERMINATED",
                    "termination_details": {"code": "UPSTREAM_FAILED", "message": "upstream"},
                },
            }
        ]
    }
    sdk, api = client({**RUN, "next_page_token": "p2"}, page2)
    run = sdk.get_run(11)
    assert api.calls[0]["method"] == "GET" and api.calls[0]["path"] == "/api/2.2/jobs/runs/get"
    assert api.calls[0]["query"] == {"run_id": 11, "include_history": "true"}
    assert api.calls[1]["query"] == {"run_id": 11, "include_history": "true", "page_token": "p2"}
    assert (run.run_id, run.job_id, run.life_cycle_state, run.result_state) == (
        11,
        5,
        "TERMINATED",
        "FAILED",
    )
    assert run.job_parameters == {"pipeloom_run_id": "r1", "full_refresh": "false"}
    assert run.latest_repair_id == 22
    assert (run.run_page_url, run.start_time_ms) == ("https://h.example/run/11", 1700000000000)
    first, second = run.tasks
    assert (first.task_key, first.run_id, first.attempt, first.message) == ("a", 12, 1, "boom")
    assert first.termination_code is None
    # `status` wins over legacy `state`; the termination code names the result state.
    assert (second.life_cycle_state, second.result_state) == ("TERMINATED", "UPSTREAM_FAILED")
    assert (second.termination_code, second.message, second.attempt) == (
        "UPSTREAM_FAILED",
        "upstream",
        0,
    )


def test_status_with_an_unknown_code_is_a_failure_and_a_running_task_has_no_result() -> None:
    sdk, _ = client(
        {
            "run_id": 1,
            "tasks": [
                {
                    "task_key": "x",
                    "run_id": 2,
                    "status": {
                        "state": "TERMINATED",
                        "termination_details": {"code": "DRIVER_ERROR"},
                    },
                },
                {"task_key": "y", "run_id": 3, "status": {"state": "RUNNING"}},
            ],
        }
    )
    x, y = sdk.get_run(1).tasks
    assert (x.result_state, x.termination_code) == ("FAILED", "DRIVER_ERROR")
    assert (y.life_cycle_state, y.result_state) == ("RUNNING", None)


def test_list_runs_pages_and_active_only() -> None:
    sdk, api = client(
        {
            "runs": [{"run_id": 1, "job_id": 5, "state": {"life_cycle_state": "RUNNING"}}],
            "next_page_token": "n",
        },
        {"runs": [{"run_id": 2, "job_id": 5, "state": {"life_cycle_state": "PENDING"}}]},
    )
    assert [r.run_id for r in sdk.list_runs(5, active_only=True)] == [1, 2]
    assert api.calls[0]["path"] == "/api/2.2/jobs/runs/list"
    assert api.calls[0]["query"] == {"job_id": 5, "limit": 25, "active_only": "true"}
    assert api.calls[1]["query"] == {
        "job_id": 5,
        "limit": 25,
        "active_only": "true",
        "page_token": "n",
    }


def test_repair_run_body() -> None:
    sdk, api = client({"repair_id": 31}, {"repair_id": 32})
    assert sdk.repair_run(11, rerun_tasks=["a", "b"], latest_repair_id=None) == 31
    assert api.calls[0]["path"] == "/api/2.2/jobs/runs/repair"
    assert api.calls[0]["body"] == {
        "run_id": 11,
        "rerun_tasks": ["a", "b"],
        "rerun_dependent_tasks": True,
    }
    sdk.repair_run(11, rerun_tasks=["a"], latest_repair_id=31)
    assert api.calls[1]["body"]["latest_repair_id"] == 31


def test_workspace_calls() -> None:
    sdk, api = client(
        {"userName": "me@example.com"},
        {},
        {},
        {
            "objects": [
                {"path": "/x/a", "object_type": "DIRECTORY", "modified_at": 5},
                {"path": "/x/f", "object_type": "FILE"},
            ]
        },
        {},
        {},
    )
    assert sdk.current_user() == "me@example.com"
    sdk.mkdirs("/x/a")
    sdk.upload("/x/f", b"\xc3\xa7 data")
    assert sdk.list_dir("/x") == [DirEntry("/x/a", True, 5), DirEntry("/x/f", False, 0)]
    sdk.delete("/x/a", recursive=True)
    assert [(c["method"], c["path"]) for c in api.calls[:3]] == [
        ("GET", "/api/2.0/preview/scim/v2/Me"),
        ("POST", "/api/2.0/workspace/mkdirs"),
        ("POST", "/api/2.0/workspace/import"),
    ]
    assert api.calls[1]["body"] == {"path": "/x/a"}
    assert api.calls[2]["body"] == {
        "path": "/x/f",
        "format": "AUTO",
        "content": base64.b64encode(b"\xc3\xa7 data").decode("ascii"),
        "overwrite": True,
    }
    assert (api.calls[3]["method"], api.calls[3]["path"], api.calls[3]["query"]) == (
        "GET",
        "/api/2.0/workspace/list",
        {"path": "/x"},
    )
    assert api.calls[4]["path"] == "/api/2.0/workspace/delete"
    assert api.calls[4]["body"] == {"path": "/x/a", "recursive": True}


def test_list_dir_of_an_empty_folder() -> None:
    sdk, _ = client({})
    assert sdk.list_dir("/x") == []


def test_read_file_streams_raw_bytes_and_quotes_the_path() -> None:
    stream = io.BytesIO(b"{}")
    sdk, api = client({"contents": stream})
    assert sdk.read_file("/Volumes/main/ops/my run/run_results.json") == b"{}"
    assert stream.closed
    assert api.calls[0] == {
        "method": "GET",
        "path": "/api/2.0/fs/files/Volumes/main/ops/my%20run/run_results.json",
        "query": None,
        "body": None,
        "raw": True,
    }


def test_get_secret_returns_the_base64_value() -> None:
    sdk, api = client({"key": "k", "value": "c2VjcmV0"})
    assert sdk.get_secret("scope", "k") == "c2VjcmV0"
    assert api.calls[0]["path"] == "/api/2.0/secrets/get"
    assert api.calls[0]["query"] == {"scope": "scope", "key": "k"}


def test_databricks_errors_become_api_errors_with_masked_messages() -> None:
    sdk, _ = client(
        NotFound("Job 7 does not exist.", error_code="RESOURCE_DOES_NOT_EXIST"),
        PermissionDenied("token hunter2-secret rejected", error_code="PERMISSION_DENIED"),
        DatabricksError("odd"),
        redactor=Redactor(("hunter2-secret",)),
    )
    with pytest.raises(ApiError) as missing:
        sdk.reset_job(7, {})
    assert (missing.value.status, missing.value.error_code) == (404, "RESOURCE_DOES_NOT_EXIST")
    assert missing.value.message == "Job 7 does not exist."
    with pytest.raises(ApiError) as denied:
        sdk.current_user()
    assert denied.value.status == 403
    assert "hunter2-secret" not in denied.value.message and "***" in denied.value.message
    with pytest.raises(ApiError) as other:
        sdk.mkdirs("/x")
    assert other.value.status == 0
