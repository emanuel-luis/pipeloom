from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from pipeloom_airflow.commands import CoreCommand, RunContext, run_context, venv_python
from pipeloom_airflow.errors import PipeloomAirflowError

AFTER = datetime(2026, 11, 5, 10, 0, tzinfo=UTC)


def ctx(**kw: object) -> RunContext:
    args: dict[str, object] = {
        "dag_id": "shop",
        "airflow_run_id": "manual__x",
        "run_after": AFTER,
        "task_id": "stg_orders",
        "try_number": 2,
        "group_id": None,
        "full_refresh": False,
    }
    args.update(kw)
    return run_context(**args)  # type: ignore[arg-type]


def test_run_context() -> None:
    c = ctx()
    assert c.run_id.startswith("af-") and len(c.run_id) == 19
    assert c.run_started_at == AFTER and c.full_refresh is False
    assert c.orchestrator_ids == {
        "airflow_dag_id": "shop",
        "airflow_run_id": "manual__x",
        "airflow_task_id": "stg_orders",
        "airflow_try_number": "2",
    }
    assert ctx(group_id="g").run_id != c.run_id


def test_run_after_is_normalised_to_utc() -> None:
    plus2 = datetime(2026, 11, 5, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    assert ctx(run_after=plus2).run_started_at == AFTER
    assert ctx(run_after=datetime(2026, 11, 5, 10, 0)).run_started_at == AFTER


@pytest.mark.parametrize(
    ("value", "expected"), [(True, True), ("true", True), ("False", False), (False, False)]
)
def test_full_refresh_accepts_booleans(value: object, expected: bool) -> None:
    assert ctx(full_refresh=value).full_refresh is expected


@pytest.mark.parametrize("value", ["yes", 1, None, ""])
def test_full_refresh_rejects_the_rest(value: object) -> None:
    with pytest.raises(PipeloomAirflowError) as err:
        ctx(full_refresh=value)
    assert err.value.diagnostics[0].code == "PLA002"


def test_a_long_airflow_run_id_is_rejected_as_an_orchestrator_id() -> None:
    with pytest.raises(ValueError, match="256"):
        ctx(airflow_run_id="x" * 300)


def command(manifest: str | None, vars_json: str | None = None) -> CoreCommand:
    return CoreCommand("/proj", "prod", manifest, "a" * 64, vars_json)


RUN_HEAD = ["py", "-m", "pipeloom", "run", "--log-format", "json", "--project-dir", "/proj"]
IDS = [
    "--orchestrator-id",
    "airflow_dag_id=shop",
    "--orchestrator-id",
    "airflow_run_id=manual__x",
    "--orchestrator-id",
    "airflow_task_id=stg_orders",
    "--orchestrator-id",
    "airflow_try_number=2",
]


def test_run_argv_with_a_manifest() -> None:
    c = ctx()
    assert command("/m.json").run_argv(["py"], "stg_orders", c, log_format="json") == [
        *RUN_HEAD,
        "--target",
        "prod",
        "--manifest",
        "/m.json",
        "--expect-plan-hash",
        "a" * 64,
        "--select",
        "unit:stg_orders",
        "--run-id",
        c.run_id,
        "--run-started-at",
        "2026-11-05T10:00:00+00:00",
        "--full-refresh",
        "false",
        *IDS,
    ]


def test_run_argv_for_a_project_passes_no_manifest() -> None:
    c = ctx(full_refresh=True)
    argv = command(None, '{"a":1}').run_argv(["py"], "u", c, log_format="text")
    assert "--manifest" not in argv
    assert argv[5] == "text"
    assert argv[argv.index("--full-refresh") + 1] == "true"
    assert argv[-2:] == ["--vars", '{"a":1}']


def test_finalize_argv() -> None:
    c = ctx()
    argv = command("/m.json").finalize_argv(
        ["py", "-I"], c, select=["tag:a", "b"], exclude=["c"], unit_states_path="/s.json"
    )
    assert argv == [
        "py",
        "-I",
        "-m",
        "pipeloom",
        "finalize",
        "--project-dir",
        "/proj",
        "--target",
        "prod",
        "--manifest",
        "/m.json",
        "--expect-plan-hash",
        "a" * 64,
        "--run-id",
        c.run_id,
        "--run-started-at",
        "2026-11-05T10:00:00+00:00",
        "--select",
        "tag:a",
        "--select",
        "b",
        "--exclude",
        "c",
        *IDS,
        "--unit-states",
        "/s.json",
    ]
    inline = command(None).finalize_argv(["py"], c, select=[], exclude=[], unit_states_json="{}")
    assert inline[-2:] == ["--unit-states-json", "{}"] and "--manifest" not in inline
    with pytest.raises(ValueError, match="not both"):
        command(None).finalize_argv(
            ["py"], c, select=[], exclude=[], unit_states_path="p", unit_states_json="{}"
        )


def test_venv_python() -> None:
    assert venv_python("/opt/venv", platform="linux") == "/opt/venv/bin/python"
    assert venv_python("/opt/venv", platform="darwin") == "/opt/venv/bin/python"
    assert venv_python("C:\\venvs\\x", platform="win32") == "C:\\venvs\\x\\Scripts\\python.exe"
