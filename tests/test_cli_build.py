"""pipeloom build, run --data-tests and retry inheritance (ADR-0045)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.runtime.executors.process import ChildCommand
from tests.conftest import make_project
from tests.runtime.test_runner import engine_block

PROJECT = """\
config_version: 1
name: shop
vars:
  act: fail
  want: 2
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
default_target: dev
"""
# `c` does not read `p`: only its relationships data test orders it after `p`.
MODELS = """\
models:
  - name: p
    sql: select 1 as id union all select 2
    unit_tests:
      - name: two_rows
        expect:
          rows:
            - id: 1
            - id: ${var:want}
  - name: c
    sql: select 1 as id union all select 3
    data_tests:
      - name: id_in_p
        check:
          relationships:
            column: id
            to:
              ref: p
            field: id
        on_violation: ${var:act}
      - name: id_set
        check:
          not_null:
            - id
"""


def _root(tmp_path: Path) -> Path:
    # Tables live under store/, so a later invocation (retry) reads what an earlier one wrote.
    project = PROJECT.replace("engine:\n      kind: memory", engine_block(tmp_path, {}))
    return make_project(tmp_path, {"pipeloom.yml": project, "models/m.yml": MODELS})


def _cli(root: Path, *args: str) -> Any:
    return CliRunner().invoke(
        main,
        [args[0], "--project-dir", str(root), "--log-format", "json", *args[1:]],
        env={"PIPELOOM_TARGET": ""},
    )


def _events(result: Any) -> list[dict[str, Any]]:
    return [json.loads(line) for line in result.stderr.splitlines() if line.startswith("{")]


def _results(root: Path, run_id: str) -> dict[str, Any]:
    path = root / "target" / "runs" / run_id / "run_results.json"
    doc: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return doc


def _table_entries(doc: dict[str, Any], model: str) -> list[dict[str, Any]]:
    row = next(r for r in doc["results"] if r["unique_id"].endswith(f".{model}"))
    return [e for e in row["expectations"] if e["scope"] == "table"]


def test_build_runs_unit_tests_then_data_tests(tmp_path: Path) -> None:
    root = _root(tmp_path)
    result = _cli(root, "build", "--run-id", "r1")
    assert result.exit_code == 1, result.output
    types = [e["type"] for e in _events(result)]
    assert "unit_test.passed" in types
    assert types.index("unit_test.passed") < types.index("run.started")
    doc = _results(root, "r1")
    assert doc["args"]["data_tests"] is True
    entries = {e["name"]: e for e in _table_entries(doc, "c")}
    assert entries["id_in_p"]["kind"] == "relationships"
    assert entries["id_in_p"]["failed"] == entries["id_in_p"]["value"] == 1
    assert entries["id_set"]["failed"] == 0
    c = next(r for r in doc["results"] if r["unique_id"].endswith(".c"))
    assert c["status"] == "failed" and c["error"]["code"] == "PLQ303"
    assert c["error"]["category"] == "permanent"
    assert c["rows_written"] == 2  # the data stays written


def test_a_failed_unit_test_stops_before_a_run(tmp_path: Path) -> None:
    root = _root(tmp_path)
    result = _cli(root, "build", "--vars", '{"want": 9}')
    assert result.exit_code == 1, result.output
    assert "unit_test.failed" in [e["type"] for e in _events(result)]
    assert "run.started" not in [e["type"] for e in _events(result)]
    runs = root / "target" / "runs"
    assert [p.name for p in runs.iterdir() if p.name != "_invocations"] == []


def test_build_select_and_warn(tmp_path: Path) -> None:
    root = _root(tmp_path)
    assert _cli(root, "build", "--select", "p", "--run-id", "r1").exit_code == 0
    result = _cli(root, "build", "--vars", '{"act": "warn"}', "--run-id", "r2")
    assert result.exit_code == 0, result.output
    c = next(r for r in _results(root, "r2")["results"] if r["unique_id"].endswith(".c"))
    assert "data test 'id_in_p' (relationships) failed" in c["warnings"]


def test_run_has_no_data_tests_by_default(tmp_path: Path) -> None:
    root = _root(tmp_path)
    assert _cli(root, "run", "--run-id", "r1", "--select", "p").exit_code == 0
    result = _cli(root, "run", "--run-id", "r2", "--select", "c")
    assert result.exit_code == 0, result.output
    doc = _results(root, "r2")
    assert doc["args"]["data_tests"] is False and _table_entries(doc, "c") == []


def test_hidden_data_tests_flag_on_run(tmp_path: Path) -> None:
    root = _root(tmp_path)
    result = _cli(root, "run", "--data-tests", "true", "--run-id", "r1")
    assert result.exit_code == 1, result.output
    assert _results(root, "r1")["args"]["data_tests"] is True
    assert "--data-tests" not in _cli(root, "run", "--help").output
    assert _cli(root, "run", "--data-tests", "maybe").exit_code == 2


def test_retry_inherits_data_tests(tmp_path: Path) -> None:
    root = _root(tmp_path)
    assert _cli(root, "build", "--run-id", "r1").exit_code == 1
    result = _cli(root, "retry", "--from", "r1", "--run-id", "r2", "--vars", '{"act": "warn"}')
    assert result.exit_code == 0, result.output
    doc = _results(root, "r2")
    assert doc["args"]["data_tests"] is True
    assert [e["name"] for e in _table_entries(doc, "c")] == ["id_in_p", "id_set"]


def test_the_process_executor_passes_data_tests_to_children(tmp_path: Path) -> None:
    def argv(on: bool) -> list[str]:
        cmd = ChildCommand(tmp_path, "dev", tmp_path / "m.json", "r1", "0" * 64, False, None, ())
        return ChildCommand(**{**cmd.__dict__, "data_tests": on}).argv("c")

    on = argv(True)
    assert on[on.index("--data-tests") + 1] == "true"
    assert "--data-tests" not in argv(False)
