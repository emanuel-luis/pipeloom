"""Model contracts (ADR-0044): config validation, manifest and hashes, and the runtime order."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeloom.api import Project, ProjectOptions
from pipeloom.docs.render import render_site
from pipeloom.events.bus import EventBus
from pipeloom.plan.manifest import manifest_dict
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project

PROJECT = """\
config_version: 1
name: shop
targets:
  dev:
    engine:
      kind: memory
      root: store
    catalog:
      kind: memory
      root: store
    naming:
      kind: none
"""
MODEL = """\
models:
  - name: a
    sql: select 1 as id, 'x' as label union all select null as id, 'y' as label
"""


def _project(tmp_path: Path, extra: str = "") -> Path:
    return make_project(tmp_path, {"pipeloom.yml": PROJECT, "models/m.yml": MODEL + extra})


def _options(root: Path) -> ProjectOptions:
    return ProjectOptions(project_dir=root, env={"PIPELOOM_USER": "tester"})


def _compile(root: Path) -> dict[str, Any]:
    outcome = Project(_options(root)).compile()
    assert outcome.value is not None, outcome.diagnostics
    return manifest_dict(outcome.value)


def _run(root: Path) -> dict[str, Any]:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    project = Project(_options(root), clock=clock, invocation_id="inv")
    with EventBus([], clock=clock, invocation_id="inv") as bus:
        project.run(RunOptions(run_id="r1", workers=1), bus)
    path = root / "target" / "runs" / "r1" / "run_results.json"
    (row,) = json.loads(path.read_bytes())["results"]
    out: dict[str, Any] = row
    return out


CONTRACT = """\
    columns:
      - name: id
        dtype: int64
        nullable: false
      - name: label
        dtype: string
"""
DROP_NULL_IDS = """\
    expectations:
      - name: id_set
        check:
          not_null:
            - id
        on_violation: drop
"""


def test_columns_enter_the_manifest_checksum_and_plan_hash_only_when_declared(
    tmp_path: Path,
) -> None:
    plain = _compile(_project(tmp_path / "plain"))
    with_contract = _compile(_project(tmp_path / "contract", CONTRACT))
    node, other = plain["nodes"]["model.shop.a"], with_contract["nodes"]["model.shop.a"]
    assert "columns" not in node
    assert other["columns"] == [
        {"name": "id", "dtype": "int64", "nullable": False},
        {"name": "label", "dtype": "string", "nullable": True},
    ]
    assert node["checksum"] != other["checksum"]
    assert plain["metadata"]["plan_hash"] != with_contract["metadata"]["plan_hash"]


def test_the_docs_site_shows_the_columns(tmp_path: Path) -> None:
    outcome = Project(_options(_project(tmp_path, CONTRACT))).compile()
    assert outcome.value is not None
    html = render_site(outcome.value)
    assert "<h3>Columns (contract)</h3>" in html
    assert "<tr><td>id</td><td><code>int64</code></td><td>no</td></tr>" in html


def _validate(root: Path) -> list[tuple[str, str]]:
    return [(d.code, d.message) for d in Project(_options(root)).validate().diagnostics]


def test_bad_dtype_and_duplicate_names_are_pls102(tmp_path: Path) -> None:
    bad = "    columns:\n      - name: id\n        dtype: integer\n"
    ((code, message),) = _validate(_project(tmp_path / "dtype", bad))
    assert code == "PLS102" and "'dtype'" in message
    dup = "    columns:\n      - name: id\n        dtype: int64\n      - name: id\n        dtype: int64\n"
    ((code, message),) = _validate(_project(tmp_path / "dup", dup))
    assert code == "PLS102" and "duplicate column name(s) id" in message


def test_a_null_in_a_non_nullable_column_fails_without_writing(tmp_path: Path) -> None:
    row = _run(_project(tmp_path, CONTRACT))
    assert row["status"] == "failed"
    assert row["error"]["code"] == "PLM208"
    assert "'id' is declared nullable: false" in row["error"]["message"]
    assert row["rows_written"] is None


def test_an_engine_error_in_the_contract_check_keeps_its_transient_category(
    tmp_path: Path,
) -> None:
    flaky = PROJECT.replace("kind: memory", "kind: flaky").replace("      root: store\n", "")
    root = make_project(tmp_path, {"pipeloom.yml": flaky, "models/m.yml": MODEL + CONTRACT})
    options = ProjectOptions(
        project_dir=root,
        env={"PIPELOOM_USER": "tester"},
        plugins=("tests.runtime.fakes:FLAKY_ENGINE", "tests.runtime.fakes:FLAKY_CATALOG"),
    )
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    with EventBus([], clock=clock, invocation_id="inv") as bus:
        outcome = Project(options, clock=clock, invocation_id="inv").run(
            RunOptions(run_id="r1", workers=1), bus
        )
    (diag,) = outcome.diagnostics
    assert diag.code == "PLM201" and diag.context["category"] == "transient"


def test_a_drop_expectation_runs_before_the_contract(tmp_path: Path) -> None:
    row = _run(_project(tmp_path, CONTRACT + DROP_NULL_IDS))
    assert row["status"] == "succeeded", row.get("error")
    assert row["rows_written"] == 1
