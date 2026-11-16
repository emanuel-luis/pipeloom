"""`pipeloom graph --units --output json`: the unit graph of ADR-0049 and the protocol it drives."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner, Result
from jsonschema import Draft202012Validator

from pipeloom.artifacts.schema import close, unit_graph_schema
from pipeloom.artifacts.unit_graph import asset_uri
from pipeloom.cli import main
from tests.conftest import FIXTURES, make_project
from tests.runtime.fakes import reset_keep
from tests.runtime.test_runner import shop
from tests.test_cli_run import ENV

SHOP = ["--project-dir", str(FIXTURES / "shop"), "--target", "ci"]
STRICT = Draft202012Validator(close(unit_graph_schema()))


def invoke(*args: str) -> Result:
    return CliRunner().invoke(main, list(args), env=ENV)


def units(*args: str) -> dict[str, Any]:
    r = invoke("graph", *args, "--units", "--output", "json")
    assert r.exit_code == 0, r.stderr
    doc: dict[str, Any] = json.loads(r.stdout)
    STRICT.validate(doc)
    return doc


def compile_to(project: list[str], out: Path) -> dict[str, Any]:
    r = invoke("compile", *project, "--stable", "--out", str(out))
    assert r.exit_code == 0, r.stderr
    manifest: dict[str, Any] = json.loads(out.read_bytes().decode("utf-8"))
    return manifest


def test_schema_is_valid() -> None:
    Draft202012Validator.check_schema(unit_graph_schema())
    Draft202012Validator.check_schema(close(unit_graph_schema()))


def test_units_golden(golden: Any) -> None:
    doc = units(*SHOP)
    order = [u["unit_id"] for u in doc["units"]]
    # Kahn with a min-heap: the smallest ready unit id goes next.
    assert order == [
        "stg_customers",
        "stg_orders",
        "dim_customers",
        "stg_payments",
        "fct_orders",
        "rpt_revenue",
    ]
    doc["pipeloom_version"] = "<version>"
    text = json.dumps(doc, indent=2, ensure_ascii=True) + "\n"
    golden("graph/units-shop.json", text.replace(doc["plan_hash"], "<plan_hash>"))


def test_manifest_gives_its_plan_hash(tmp_path: Path) -> None:
    manifest = compile_to(SHOP, tmp_path / "manifest.json")
    doc = units(*SHOP, "--manifest", str(tmp_path / "manifest.json"))
    assert doc["plan_hash"] == manifest["metadata"]["plan_hash"]
    assert doc == units(*SHOP)


def test_select_narrows_units_and_edges(tmp_path: Path) -> None:
    compile_to(SHOP, tmp_path / "manifest.json")
    for extra in ([], ["--manifest", str(tmp_path / "manifest.json")]):
        doc = units(*SHOP, *extra, "--select", "fct_orders+", "--exclude", "tag:staging")
        assert [u["unit_id"] for u in doc["units"]] == ["fct_orders", "rpt_revenue"]
        assert [u["parents"] for u in doc["units"]] == [[], ["fct_orders"]]
        assert doc["edges"] == [{"from": "fct_orders", "to": "rpt_revenue"}]
        assert doc["selectors"] == {"select": ["fct_orders+"], "exclude": ["tag:staging"]}
        argv = doc["finalize"]["argv"]
        assert argv[argv.index("--select") + 1] == "fct_orders+"
        assert argv[argv.index("--exclude") + 1] == "tag:staging"


def test_state_selectors_become_units_in_finalize(tmp_path: Path) -> None:
    """finalize has no --state, so a state: or result: selection reaches it as unit: selectors."""
    root = tmp_path / "shop"
    shutil.copytree(FIXTURES / "shop", root, ignore=shutil.ignore_patterns("target"))
    project = ["--project-dir", str(root), "--target", "ci"]
    compile_to(project, tmp_path / "ref" / "manifest.json")
    marts = root / "models" / "marts" / "marts.yml"
    text = marts.read_bytes().decode("utf-8").replace("sum(amount)", "sum(amount) + 0")
    marts.write_bytes(text.encode("utf-8"))
    state = ["--state", str(tmp_path / "ref")]
    doc = units(*project, *state, "-s", "state:modified", "-s", "stg_orders")
    assert [u["unit_id"] for u in doc["units"]] == ["rpt_revenue", "stg_orders"]  # no edge
    assert doc["selectors"]["select"] == ["state:modified", "stg_orders"]
    argv = doc["finalize"]["argv"]
    assert "state:modified" not in argv
    picked = [argv[i + 1] for i, a in enumerate(argv) if a == "--select"]
    assert picked == ["unit:rpt_revenue", "unit:stg_orders"]
    manifest = root / "target" / "manifest.json"
    compile_to(project, manifest)
    values = {"{project_dir}": str(root), "{manifest}": str(manifest), "{run_id}": "r1"}
    r = invoke(*(values.get(a, a) for a in argv[1:]))
    assert r.exit_code == 0, r.stderr
    nothing = units(*project, *state, "--select", "state:new")
    argv = nothing["finalize"]["argv"]
    assert nothing["units"] == [] and argv[argv.index("--exclude") + 1] == "*"
    values["{run_id}"] = "r2"
    r = invoke(*(values.get(a, a) for a in argv[1:]))
    assert r.exit_code == 0, r.stderr


PLUGIN_YML = """config_version: 1
name: p
targets:
  ci:
    engine:
      kind: keepmem
    catalog:
      kind: keepmem
"""
PLUGIN_MODELS = """models:
  - name: a
    sql: select 1 as x
  - name: b
    sql: select * from a
    inputs:
      a:
        ref: a
"""


def test_plugin_specs_reach_every_argv(tmp_path: Path) -> None:
    """A kind loaded with --plugin binds only when each command line names it again."""
    reset_keep()
    root = make_project(tmp_path, {"pipeloom.yml": PLUGIN_YML, "models/m.yml": PLUGIN_MODELS})
    plugins = ["--plugin", "tests.runtime.fakes:KEEP_ENGINE"]
    plugins += ["--plugin", "tests.runtime.fakes:KEEP_CATALOG"]
    project = ["--project-dir", str(root), "--target", "ci", *plugins]
    manifest = root / "target" / "manifest.json"
    compile_to(project, manifest)
    doc = units(*project, "--manifest", str(manifest))
    argvs = [u["argv"] for u in doc["units"]] + [doc["finalize"]["argv"]]
    values = {"{project_dir}": str(root), "{manifest}": str(manifest), "{run_id}": "r1"}
    for argv in argvs:
        assert argv[-4:] == plugins
        r = invoke(*(values.get(a, a) for a in argv[1:]))
        assert r.exit_code == 0, r.stderr


@pytest.mark.parametrize("output", [None, "text", "dot", "mermaid"])
def test_units_needs_json(output: str | None) -> None:
    args = ["graph", *SHOP, "--units"] + ([] if output is None else ["--output", output])
    r = invoke(*args)
    assert r.exit_code == 2 and r.stdout == ""
    assert "--units needs --output json" in r.stderr


def test_manifest_needs_units(tmp_path: Path) -> None:
    compile_to(SHOP, tmp_path / "manifest.json")
    r = invoke("graph", *SHOP, "--manifest", str(tmp_path / "manifest.json"))
    assert r.exit_code == 2 and r.stdout == ""
    assert "--manifest needs --units" in r.stderr


def test_bad_selector_exits_2(tmp_path: Path) -> None:
    compile_to(SHOP, tmp_path / "manifest.json")
    for extra in ([], ["--manifest", str(tmp_path / "manifest.json")]):
        r = invoke("graph", *SHOP, *extra, "--units", "--output", "json", "--select", "nme:x")
        assert r.exit_code == 2 and r.stdout == ""
        assert "PLG304" in r.stderr


def test_asset_uri_percent_encodes_each_part() -> None:
    assert asset_uri("p", "dev", "c.s.t") == "pipeloom://p/dev/c.s.t"
    assert asset_uri("p", "a b", "x/y") == "pipeloom://p/a%20b/x%2Fy"


def test_the_argvs_run_the_project_end_to_end(tmp_path: Path) -> None:
    """The protocol of ADR-0049 with nothing but the unit graph: every unit argv in order, then
    the finalize argv, with the placeholders filled in."""
    root = shop(tmp_path / "shop")
    project = ["--project-dir", str(root), "--target", "ci"]
    manifest = root / "target" / "manifest.json"
    compile_to(project, manifest)
    doc = units(*project, "--manifest", str(manifest))
    assert doc["placeholders"] == ["project_dir", "manifest", "run_id"]
    values = {"{project_dir}": str(root), "{manifest}": str(manifest), "{run_id}": "r1"}

    def call(argv: list[str]) -> None:
        assert argv[0] == "pipeloom"
        r = invoke(*(values.get(a, a) for a in argv[1:]))
        assert r.exit_code == 0, r.stderr

    for unit in doc["units"]:
        call(unit["argv"])
    call(doc["finalize"]["argv"])
    results = root / "target" / "runs" / "r1" / "run_results.json"
    data = json.loads(results.read_bytes().decode("utf-8"))
    assert data["run"]["status"] == "succeeded"
    assert sorted(r["unit_id"] for r in data["results"]) == sorted(
        u["unit_id"] for u in doc["units"]
    )
    assert {r["status"] for r in data["results"]} == {"succeeded"}
