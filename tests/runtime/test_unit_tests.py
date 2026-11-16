"""Unit tests (ADR-0045): config, the case runner on the memory engine, selection, and
manifests that stay byte-identical."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from pipeloom.api import Project, ProjectOptions
from pipeloom.config.models import GivenConfig, UnitTestConfig
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import manifest_bytes
from pipeloom.platform import Identity
from pipeloom.runtime.context import RuntimeContext
from pipeloom.runtime.unit_tests import run_unit_test
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project

PROJECT = """\
config_version: 1
name: shop
python_paths:
  - src
model_paths:
  - models
  - tests
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
"""
MODELS = """\
sources:
  - name: raw
    location: raw
models:
  - name: b
    sql: select id, amount from src
    inputs:
      src:
        source: raw
    columns:
      - name: id
        dtype: string
      - name: amount
        dtype: float64
  - name: c
    python: m:build
    inputs:
      b:
        ref: b
    unit_tests:
      - name: doubles
        given:
          b:
            rows:
              - id: x
                amount: 1.5
              - id: y
                amount: null
        expect:
          rows:
            - id: y
              doubled: null
            - id: x
              doubled: 3.0
"""
BODY = """\
def build(ctx, b):
    assert not ctx.is_incremental
    return [
        {"id": r["id"], "doubled": None if r["amount"] is None else r["amount"] * 2, "n": 1}
        for r in b
    ]
"""
FILE_CASES = """\
unit_tests:
  - name: from_csv
    model: b
    given:
      src:
        csv: tests/raw.csv
    expect:
      csv: tests/expected.csv
  - name: wrong
    model: b
    given:
      src:
        rows:
          - id: 1
            amount: 2
    expect:
      rows:
        - id: '1'
          amount: 3
"""


def _project(tmp_path: Path) -> Path:
    return make_project(
        tmp_path,
        {
            "pipeloom.yml": PROJECT,
            "models/m.yml": MODELS,
            "src/m.py": BODY,
            "tests/cases.yml": FILE_CASES,
            # CSV ids infer as int64; expected values cast to the output types
            "tests/raw.csv": "id,amount\n1,2.5\n2,\n",
            "tests/expected.csv": "amount,id\n2.5,1\n,2\n",
        },
    )


def _test(root: Path, select: tuple[str, ...] = ()) -> Any:
    return Project(ProjectOptions(project_dir=root, env={})).test(Selection(select))


def test_cases_pass_and_fail_with_a_row_diff(tmp_path: Path) -> None:
    outcome = _test(_project(tmp_path))
    by_name = {e.unit_test: e for e in outcome.events if hasattr(e, "unit_test")}
    assert by_name["from_csv"].TYPE == "unit_test.passed"
    assert by_name["doubles"].TYPE == "unit_test.passed"
    wrong = by_name["wrong"]
    assert wrong.code == "PLX114"
    assert wrong.missing == [{"id": 1, "amount": 3}]
    assert wrong.unexpected == [{"id": 1, "amount": 2}]
    assert "1 expected row(s) missing" in wrong.message
    assert wrong.location["file"] == "tests/cases.yml"
    assert outcome.events[-1].TYPE == "unit_tests.completed"
    assert (outcome.events[-1].passed, outcome.events[-1].failed) == (2, 1)
    assert outcome.exit_code == 1 and outcome.failed == 1
    assert not (tmp_path / "target").exists()  # nothing written, no run


def test_selection_limits_the_cases(tmp_path: Path) -> None:
    outcome = _test(_project(tmp_path), ("c",))
    assert [e.TYPE for e in outcome.events] == ["unit_test.passed", "unit_tests.completed"]
    assert outcome.exit_code == 0


def test_an_invalid_project_runs_nothing(tmp_path: Path) -> None:
    root = _project(tmp_path)
    cases = root / "tests" / "cases.yml"
    cases.write_text(cases.read_text(encoding="utf-8").replace("src:", "nope:"), encoding="utf-8")
    outcome = _test(root)
    assert outcome.exit_code == 1 and outcome.events == ()
    assert "PLR208" in {d.code for d in outcome.diagnostics}


class _Bare:
    """A session from an engine without the plugin API 0.5 row methods."""


def test_an_engine_without_from_rows_cannot_run(tmp_path: Path) -> None:
    root = _project(tmp_path)
    plan = Project(ProjectOptions(project_dir=root, env={})).compile().value
    assert plan is not None
    rt = RuntimeContext(
        run_id="t",
        invocation_id="t",
        project="shop",
        target="dev",
        project_dir=root,
        clock=FakeClock(),
        identity=Identity("tester", "host"),
        vars={},
    )
    case = UnitTestConfig(
        name="x", given={"src": GivenConfig(rows=[])}, expect=GivenConfig(rows=[])
    )
    out = run_unit_test(_Bare(), plan.nodes["model.shop.b"], case, load=lambda r: print, rt=rt)  # type: ignore[arg-type]
    assert out.code == "PLX115" and "from_rows" in out.message


def test_an_untyped_null_column_cannot_run(tmp_path: Path) -> None:
    root = _project(tmp_path)
    (root / "tests" / "raw.csv").write_text("id,amount\n1,\n", encoding="utf-8")
    # drop the contract of b (a `columns=` decorator keyword), so nothing types amount, which
    # holds only nulls
    (module,) = [p for p in (root / "models").glob("*.py") if "columns=" in p.read_text("utf-8")]
    lines = module.read_text(encoding="utf-8").splitlines(keepends=True)
    module.write_text("".join(ln for ln in lines if "columns=" not in ln), encoding="utf-8")
    outcome = _test(root, ("b",))
    failed = [e for e in outcome.events if e.TYPE == "unit_test.failed"]
    assert failed[0].code == "PLX115"
    assert "holds only nulls" in failed[0].message


def test_unit_tests_leave_manifests_byte_identical(tmp_path: Path) -> None:
    fixture = Path(__file__).parent.parent / "fixtures" / "shop"
    before, after = tmp_path / "before", tmp_path / "after"
    shutil.copytree(fixture, before)
    shutil.copytree(fixture, after)
    (after / "models" / "staging" / "cases.yml").write_text(
        "models:\n"
        "  - name: stg_customers\n"
        "    unit_tests:\n"
        "      - name: lower_emails\n"
        "        given:\n"
        "          raw_customers:\n"
        "            rows:\n"
        "              - id: 1\n"
        "                email: A@X.IO\n"
        "        expect:\n"
        "          rows:\n"
        "            - email: a@x.io\n",
        encoding="utf-8",
    )
    plans = []
    for root in (before, after):
        outcome = Project(ProjectOptions(project_dir=root, target="ci", env={})).compile()
        assert outcome.value is not None, outcome.diagnostics
        plans.append(outcome.value)
    assert plans[0].metadata.plan_hash == plans[1].metadata.plan_hash
    assert manifest_bytes(plans[0], stable=True) == manifest_bytes(plans[1], stable=True)
    tested = Project(ProjectOptions(project_dir=after, target="ci", env={})).test()
    assert tested.exit_code == 0, tested.events
