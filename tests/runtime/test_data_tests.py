"""Data tests in the runner (ADR-0045): results with scope table, warn, a replayed write that is
tested again, and parents read through --defer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeloom.api import Project, ProjectOptions
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import write_manifest
from pipeloom.runtime.run import RunOptions, RunOutcome
from tests.conftest import make_project
from tests.runtime.test_runner import engine_block, results

ENV = {"PIPELOOM_USER": "tester"}
MODELS = """\
models:
  - name: p
    sql: select 1 as id union all select 2
  - name: c
    sql: select 1 as id union all select 3
    expectations:
      - name: has_rows
        check:
          row_count:
            min: 1
"""
# In properties, where `${var:...}` interpolates (a decorator keyword cannot).
PROPS = """\
models:
  - name: c
    data_tests:
      - name: id_in_p
        check:
          relationships:
            column: id
            to:
              ref: p
            field: id
        on_violation: ${var:act}
      - name: id_unique
        check:
          unique:
            - id
"""


def _root(tmp_path: Path, seed: dict[str, Any] | None = None) -> Path:
    project = (
        "config_version: 1\nname: dt\nvars:\n  act: fail\n"
        + engine_block(tmp_path, seed or {})
        + "\ncatalog:\n  kind: memory\n"
        "targets:\n"
        "  prod:\n    naming:\n      kind: none\n"
        "  dev:\n    naming:\n      kind: prefix\n      schema_prefix: dev_\n"
    )
    return make_project(
        tmp_path,
        {"pipeloom.yml": project, "models/m.yml": MODELS, "models/_props.yml": PROPS},
    )


def _run(
    root: Path, *select: str, target: str = "prod", act: str = "fail", **opts: Any
) -> RunOutcome:
    project = Project(
        ProjectOptions(project_dir=root, target=target, env=ENV, vars={"act": act}),
        invocation_id="inv",
    )
    run_opts = RunOptions(Selection(select), workers=1, data_tests=True, **opts)
    return project.run(run_opts)


def _row(root: Path, run_id: str, model: str) -> dict[str, Any]:
    doc = results(root, run_id)
    row: dict[str, Any] = next(r for r in doc["results"] if r["unique_id"] == f"model.dt.{model}")
    return row


def test_results_carry_scope_and_a_failure_is_plq303(tmp_path: Path) -> None:
    root = _root(tmp_path)
    outcome = _run(root, run_id="r1")
    assert outcome.exit_code == 1
    assert {d.code for d in outcome.diagnostics} == {"PLQ303"}
    row = _row(root, "r1", "c")
    scopes = [(e["name"], e["scope"], e["failed"]) for e in row["expectations"]]
    assert scopes == [("has_rows", "batch", 0), ("id_in_p", "table", 1), ("id_unique", "table", 0)]
    assert row["rows_written"] == 2 and row["error"]["category"] == "permanent"
    assert results(root, "r1")["args"]["data_tests"] is True


def test_warn_records_the_count_and_succeeds(tmp_path: Path) -> None:
    root = _root(tmp_path)
    assert _run(root, run_id="r1", act="warn").exit_code == 0
    assert "data test 'id_in_p' (relationships) failed" in _row(root, "r1", "c")["warnings"]


def test_a_replayed_write_is_tested_again(tmp_path: Path) -> None:
    root = _root(tmp_path)
    assert _run(root, run_id="r1").exit_code == 1
    # The same run id continues the run: the write token replays and the tests run again.
    assert _run(root, "c", run_id="r1", act="warn").exit_code == 0
    row = _row(root, "r1", "c")
    entries = [(e["name"], e["scope"]) for e in row["expectations"]]
    assert entries == [("has_rows", "batch"), ("id_in_p", "table"), ("id_unique", "table")]
    assert "data test 'id_in_p' (relationships) failed" in row["warnings"]


def test_defer_reads_the_parent_from_the_reference(tmp_path: Path) -> None:
    root = _root(tmp_path, {"main.p": [{"id": 1}, {"id": 3}]})
    plan = Project(ProjectOptions(project_dir=root, target="prod", env=ENV)).compile().value
    assert plan is not None
    write_manifest(plan, root / "state" / "manifest.json", stable=True)
    outcome = _run(root, "c", target="dev", run_id="r1", state=Path("state"), defer=True)
    assert outcome.exit_code == 0, outcome.diagnostics
    entries = {e["name"]: e for e in _row(root, "r1", "c")["expectations"]}
    assert entries["id_in_p"]["failed"] == 0  # main.p holds 1 and 3
