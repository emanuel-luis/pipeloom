from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.events.model import Event
from tests.conftest import FIXTURES, make_project

MINIMAL = FIXTURES / "minimal"
BASE = "config_version: 1\nname: shop\ntargets:\n  dev:\n    engine: {kind: memory}\n"
ENV = {"PIPELOOM_TARGET": "", "PIPELOOM_LOG_FORMAT": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": ""}


def run(args: list[str], env: dict[str, str] | None = None) -> Any:
    return CliRunner().invoke(main, args, env={**ENV, **(env or {})})


def events(result: Any) -> list[Event]:
    return [Event.model_validate_json(line) for line in result.stderr.splitlines()]


def project(tmp_path: Path, extra: str = "") -> list[str]:
    make_project(tmp_path, {"pipeloom.yml": BASE + extra, "models/.keep": ""})
    return ["--project-dir", str(tmp_path)]


def test_log_format_defaults_to_text() -> None:
    result = run(["validate", "--project-dir", str(MINIMAL)])
    assert result.exit_code == 0 and not result.stderr.lstrip().startswith("{")
    assert "command validate | project minimal | target dev" in result.stderr
    assert "validate finished in" in result.stderr and "exit 0" in result.stderr


def test_log_format_json_flag_gives_jsonl_on_stderr_and_results_on_stdout() -> None:
    result = run(["validate", "--project-dir", str(MINIMAL), "--log-format", "json"])
    assert result.exit_code == 0
    assert result.stdout.strip() == "Project 'minimal' is valid."
    got = events(result)
    assert [e.type for e in got] == ["invocation.started", "invocation.completed"]
    assert got[0].data["project"] == "minimal" and got[0].data["target"] == "dev"
    assert got[1].data["exit_code"] == 0 and got[1].data["counts"] == {"errors": 0, "warnings": 0}
    assert {e.invocation_id for e in got} == {got[0].invocation_id} and got[0].run_id is None
    assert [e.seq for e in got] == [1, 2]


def test_log_format_env_var_and_flag_beats_it() -> None:
    args = ["validate", "--project-dir", str(MINIMAL)]
    assert events(run(args, env={"PIPELOOM_LOG_FORMAT": "json"}))
    flag = run([*args, "--log-format", "text"], env={"PIPELOOM_LOG_FORMAT": "json"})
    assert "command validate" in flag.stderr


def test_log_format_rejects_unknown_values() -> None:
    result = run(["validate", "--project-dir", str(MINIMAL), "--log-format", "xml"])
    assert result.exit_code == 2 and "xml" in result.stderr


def test_log_format_on_plugins_list_and_schema_export(tmp_path: Path) -> None:
    listed = run(["plugins", "list", "--output", "json", "--log-format", "json"])
    assert json.loads(listed.stdout) and events(listed)[0].data["command"] == "plugins.list"
    out = tmp_path / "s"
    exported = run(
        ["schema", "export", "--out", str(out), "--plugins", "builtin", "--log-format", "json"]
    )
    got = events(exported)
    assert [e.type for e in got] == ["invocation.started"] + ["artifact.written"] * 3 + [
        "invocation.completed"
    ]
    first = got[1].data
    assert (
        first["kind"] == "json_schema" and first["bytes"] == (out / "defaults.json").stat().st_size
    )
    assert len(str(first["sha256"])) == 64
    env_var = run(["plugins", "list"], env={"PIPELOOM_LOG_FORMAT": "json"})
    assert events(env_var)[-1].type == "invocation.completed"


def test_diagnostics_are_events_in_sorted_order_between_started_and_completed(
    tmp_path: Path,
) -> None:
    make_project(tmp_path, {"pipeloom.yml": BASE + "zzz: 1\naaa: 2\n"})
    result = run(["validate", "--project-dir", str(tmp_path), "--log-format", "json"])
    got = events(result)
    assert result.exit_code == 1 and result.stdout == ""
    assert [e.type for e in got] == [
        "invocation.started",
        "diagnostic",
        "diagnostic",
        "invocation.completed",
    ]
    assert [e.level.value for e in got[1:3]] == ["error", "error"]
    lines = [e.data["location"]["line"] for e in got[1:3]]  # type: ignore[index]
    assert lines == sorted(lines)
    assert got[-1].data["exit_code"] == 1 and got[-1].data["counts"]["errors"] == 2


def test_failed_load_has_no_project_and_still_gets_started_and_completed(tmp_path: Path) -> None:
    result = run(["validate", "--project-dir", str(tmp_path), "--log-format", "json"])
    got = events(result)
    assert result.exit_code == 1
    assert got[0].data["project"] is None and got[1].data["code"] == "PLY004"


def test_warn_error_note_on_the_completed_line(tmp_path: Path) -> None:
    make_project(tmp_path, {"pipeloom.yml": BASE})
    result = run(["validate", "--project-dir", str(tmp_path), "--warn-error"])
    assert result.exit_code == 1 and "warnings treated as errors" in result.stderr


def test_vars_values_never_reach_the_started_event() -> None:
    result = run(
        [
            "validate",
            "--project-dir",
            str(MINIMAL),
            "--vars",
            "country: hunter2hunter2",
            "--log-format",
            "json",
        ]
    )
    assert "hunter2hunter2" not in result.stderr
    assert events(result)[0].data["args"]["vars"] == ["country"]


def test_config_jsonl_sink_writes_a_file_under_the_project(tmp_path: Path) -> None:
    args = project(tmp_path, "events:\n  sinks:\n    - {kind: jsonl, path: target/events.jsonl}\n")
    result = run(["validate", *args])
    assert result.exit_code == 0
    written = (tmp_path / "target" / "events.jsonl").read_text(encoding="utf-8")
    types = [Event.model_validate_json(line).type for line in written.splitlines()]
    assert types == ["invocation.started", "invocation.completed"]
    assert "command validate" in result.stderr  # the terminal sink still prints


def test_config_sinks_come_after_the_terminal_sink_and_can_be_buffered(tmp_path: Path) -> None:
    extra = (
        "events:\n  sinks:\n    - kind: jsonl\n      path: a.jsonl\n      buffered: true\n"
        "      buffer_size: 5\n    - {kind: console}\n"
    )
    args = project(tmp_path, extra)
    result = run(["validate", *args, "--log-format", "json"])
    assert result.exit_code == 0
    assert (tmp_path / "a.jsonl").read_text(encoding="utf-8").count("\n") == 2
    assert result.stderr.count("command validate") == 1  # console sink from config
    assert result.stderr.count('"invocation.started"') == 1  # jsonl terminal sink


def test_config_sinks_are_not_used_when_the_project_does_not_bind(tmp_path: Path) -> None:
    args = project(tmp_path, "bogus: 1\nevents:\n  sinks:\n    - {kind: jsonl, path: a.jsonl}\n")
    assert run(["validate", *args]).exit_code == 1
    assert not (tmp_path / "a.jsonl").exists()


@pytest.mark.parametrize("path", ["/abs/a.jsonl", "../a.jsonl"])
def test_jsonl_sink_path_must_stay_inside_the_project(tmp_path: Path, path: str) -> None:
    args = project(tmp_path, f"events:\n  sinks:\n    - {{kind: jsonl, path: '{path}'}}\n")
    result = run(["validate", *args])
    assert result.exit_code == 1 and "PLS102" in result.stderr


def test_unknown_sink_option_is_reported_with_a_location(tmp_path: Path) -> None:
    args = project(tmp_path, "events:\n  sinks:\n    - {kind: console, colour: 1}\n")
    result = run(["validate", *args])
    assert result.exit_code == 1 and "PLS101" in result.stderr


def test_a_failing_config_sink_is_disabled_and_exit_code_is_unchanged(tmp_path: Path) -> None:
    args = project(tmp_path, "events:\n  sinks:\n    - {kind: boom, failure_budget: 1}\n")
    result = run(["validate", *args, "--plugin", "tests.events.fakes:BOOM_SINK"])
    assert result.exit_code == 0
    assert "warning PLE401 sink 'boom' disabled" in result.stderr


def test_no_diagnostic_printer_module_is_left() -> None:
    import importlib.util

    assert importlib.util.find_spec("pipeloom.cli._output") is None
