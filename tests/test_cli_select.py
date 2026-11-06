from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from tests.conftest import FIXTURES, make_project

FILES = {
    "pipeloom.yml": (
        "config_version: 1\nname: shop\ntargets:\n  dev:\n    engine: {kind: memory}\n"
    ),
    "models/staging.yml": (
        "sources:\n"
        "  - {name: raw, format: memory, location: raw/x}\n"
        "models:\n"
        "  - {name: stg_a, sql: select 1, tags: [staging, café], inputs: {r: {source: raw}}}\n"
        "  - {name: stg_b, sql: select 1, tags: [staging], inputs: {r: {source: raw}}}\n"
    ),
    "models/marts/mart.yml": (
        "models:\n"
        "  - name: mart\n"
        "    sql: select 1\n"
        "    materialized: view\n"
        "    tags: [mart]\n"
        "    inputs: {a: {ref: stg_a}, b: {ref: stg_b}}\n"
    ),
    "models/reports/report.yml": (
        "models:\n  - {name: report, sql: select 1, inputs: {m: {ref: mart}}}\n"
    ),
}


@pytest.fixture
def project(tmp_path: Path) -> list[str]:
    make_project(tmp_path, FILES)
    return ["--project-dir", str(tmp_path)]


def run(args: list[str]) -> Any:
    return CliRunner().invoke(main, args, env={})


def lines(result: Any) -> list[str]:
    return result.stdout.splitlines()


def test_ls_text_is_sorted_names(project: list[str]) -> None:
    r = run(["ls", *project])
    assert r.exit_code == 0
    assert lines(r) == ["mart", "report", "stg_a", "stg_b"]


def test_ls_json_rows_and_ascii(project: list[str]) -> None:
    r = run(["ls", *project, "--output", "json", "--select", "stg_a"])
    assert r.exit_code == 0
    assert "\\u00e9" in r.stdout and "é" not in r.stdout
    assert json.loads(r.stdout) == [
        {
            "unique_id": "model.shop.stg_a",
            "name": "stg_a",
            "materialized": "table",
            "tags": ["staging", "café"],
            "path": "models/staging.yml",
        }
    ]


def test_select_repeated_is_union(project: list[str]) -> None:
    r = run(["ls", *project, "-s", "stg_a", "--select", "report"])
    assert lines(r) == ["report", "stg_a"]


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["--select", "tag:staging"], ["stg_a", "stg_b"]),
        (["--select", "path:models/marts"], ["mart"]),
        (["--select", "materialized:view"], ["mart"]),
        (["--select", "+mart"], ["mart", "stg_a", "stg_b"]),
        (["--select", "stg_a+1"], ["mart", "stg_a"]),
        (["--select", "tag:staging,stg_b"], ["stg_b"]),
        (["--exclude", "tag:staging"], ["mart", "report"]),
        (["--select", "+report", "--exclude", "mart+"], ["stg_a", "stg_b"]),
        (["--exclude", "stg_a", "--exclude", "stg_b"], ["mart", "report"]),
    ],
)
def test_select_and_exclude(project: list[str], args: list[str], expected: list[str]) -> None:
    r = run(["ls", *project, *args])
    assert r.exit_code == 0, r.stderr
    assert lines(r) == expected


@pytest.mark.parametrize(("selector", "code"), [("a,,b", "PLG305"), ("nme:x", "PLG304")])
def test_bad_selector_exits_2(project: list[str], selector: str, code: str) -> None:
    for command in ("ls", "graph"):
        r = run([command, *project, "--select", selector])
        assert r.exit_code == 2
        assert code in r.stderr and r.stdout == ""
    r = run(["ls", *project, "--exclude", selector])
    assert r.exit_code == 2


def test_nothing_matched_is_a_warning(project: list[str]) -> None:
    r = run(["ls", *project, "--select", "tag:nope"])
    assert r.exit_code == 0
    assert r.stdout == ""
    assert "PLG303" in r.stderr


def test_graph_text(project: list[str]) -> None:
    r = run(["graph", *project])
    assert lines(r) == ["stg_a", "stg_b", "mart <- stg_a, stg_b", "report <- mart"]


def test_graph_is_induced_subgraph(project: list[str]) -> None:
    r = run(["graph", *project, "--select", "stg_a mart"])
    assert lines(r) == ["stg_a", "mart <- stg_a"]


def test_graph_json(project: list[str]) -> None:
    r = run(["graph", *project, "--output", "json", "--select", "mart+"])
    data = json.loads(r.stdout)
    assert [n["name"] for n in data["nodes"]] == ["mart", "report"]
    assert data["edges"] == [{"from": "model.shop.mart", "to": "model.shop.report"}]


def test_graph_dot_and_mermaid_goldens(
    project: list[str], golden: Callable[[str, str], None]
) -> None:
    dot = run(["graph", *project, "--output", "dot"])
    mermaid = run(["graph", *project, "--output", "mermaid"])
    assert dot.exit_code == mermaid.exit_code == 0
    golden("graph/small.dot", dot.stdout)
    golden("graph/small.mmd", mermaid.stdout)


def test_graph_empty_selection(project: list[str]) -> None:
    r = run(["graph", *project, "--select", "tag:nope", "--output", "mermaid"])
    assert r.exit_code == 0
    assert lines(r) == ["graph LR"]


def test_validate_runs_l2_and_l3(tmp_path: Path) -> None:
    files = dict(FILES)
    files["models/marts/mart.yml"] = files["models/marts/mart.yml"].replace(
        "ref: stg_a", "ref: stg_aa"
    )
    make_project(tmp_path, files)
    r = run(["validate", "--project-dir", str(tmp_path)])
    assert r.exit_code == 1 and "PLR201" in r.stderr

    files["models/marts/mart.yml"] = files["models/marts/mart.yml"].replace("stg_aa", "report")
    make_project(tmp_path, files)
    r = run(["validate", "--project-dir", str(tmp_path)])
    assert r.exit_code == 1 and "PLG301" in r.stderr


def test_validate_ok_on_graph_project(tmp_path: Path) -> None:
    files = dict(FILES)
    files["models/marts/mart.yml"] = files["models/marts/mart.yml"].replace("view", "table")
    make_project(tmp_path, files)
    r = run(["validate", "--project-dir", str(tmp_path)])
    assert r.exit_code == 0
    assert r.stdout.strip() == "Project 'shop' is valid."


def test_validate_flags_a_view_on_an_engine_without_views(tmp_path: Path) -> None:
    files = dict(FILES)
    files["pipeloom.yml"] = files["pipeloom.yml"].replace("kind: memory", "kind: nosql")
    make_project(tmp_path, files)
    args = ["validate", "--project-dir", str(tmp_path), "--plugin", "tests.plugins.fakes:NOSQL"]
    r = run(args)
    assert r.exit_code == 1 and "PLC507" in r.stderr


def test_validate_accepts_a_view_on_the_memory_engine(project: list[str]) -> None:
    assert run(["validate", *project]).exit_code == 0


def test_ls_events_go_to_stderr(project: list[str]) -> None:
    r = run(["ls", *project, "--log-format", "json"])
    types = [json.loads(line)["type"] for line in r.stderr.splitlines()]
    assert types[0] == "invocation.started" and types[-1] == "invocation.completed"


def test_ls_output_text_explicit(project: list[str]) -> None:
    explicit = run(["ls", *project, "--output", "text", "--select", "stg_a stg_b"])
    assert explicit.exit_code == 0 and lines(explicit) == ["stg_a", "stg_b"]
    assert explicit.stdout == run(["ls", *project, "--select", "stg_a stg_b"]).stdout


def test_graph_output_text_explicit(project: list[str]) -> None:
    explicit = run(["graph", *project, "--output", "text", "--select", "stg_a mart"])
    assert explicit.exit_code == 0 and lines(explicit) == ["stg_a", "mart <- stg_a"]


# -- selection by state -----------------------------------------------------------------------


@pytest.fixture
def shop(tmp_path: Path) -> Path:
    """The shop fixture with its `ci` manifest compiled as the stable reference in state/."""
    root = tmp_path / "shop"
    shutil.copytree(FIXTURES / "shop", root, ignore=shutil.ignore_patterns("__pycache__"))
    done = run(
        ["compile", "--project-dir", str(root), "--target", "ci", "--stable"]
        + ["--out", "state/manifest.json"]
    )
    assert done.exit_code == 0, done.stderr
    return root


def edit(root: Path, file: str, old: str, new: str) -> None:
    path = root / file
    text = path.read_bytes().decode("utf-8")
    assert old in text
    path.write_bytes(text.replace(old, new).encode("utf-8"))


def modified(root: Path, expr: str = "state:modified", *extra: str) -> list[str]:
    # dev uses per_user naming: the comparison must not see the target's relations
    r = run(["ls", "--project-dir", str(root), "--target", "dev", "--state", "state", "-s", expr])
    assert r.exit_code == 0, r.stderr
    return lines(r)


def test_nothing_changed_selects_nothing(shop: Path) -> None:
    assert modified(shop) == []
    assert modified(shop, "state:new") == []


def test_changing_one_sql_body_selects_exactly_that_node(shop: Path) -> None:
    edit(shop, "models/staging/stg.yml", "select id, customer_id, amount", "select id, amount")
    assert modified(shop) == ["stg_orders"]
    assert modified(shop, "state:modified+") == [
        "dim_customers",
        "fct_orders",
        "orders_enriched",
        "rpt_revenue",
        "stg_orders",
    ]


def test_rewired_inputs_count_as_a_change(shop: Path) -> None:
    edit(shop, "models/marts/marts.yml", "ref: stg_payments", "ref: stg_customers")
    assert modified(shop) == ["fct_orders"]


def test_a_modified_ephemeral_model_marks_the_models_that_inline_it(shop: Path) -> None:
    edit(shop, "src/shop/enrich.py", "return orders", "return customers")
    assert modified(shop) == ["dim_customers", "fct_orders", "orders_enriched"]


def test_a_new_model_is_new_and_modified(shop: Path) -> None:
    edit(
        shop,
        "models/marts/marts.yml",
        "  - name: rpt_revenue",
        "  - name: rpt_extra\n    sql: select 1\n  - name: rpt_revenue",
    )
    assert modified(shop, "state:new") == ["rpt_extra"]
    assert modified(shop) == ["rpt_extra"]


def test_state_is_relative_to_the_project_dir_or_absolute(shop: Path) -> None:
    edit(shop, "models/staging/stg.yml", "select id, customer_id, amount", "select id, amount")
    for state in ("state", str(shop / "state")):
        r = run(
            [
                "ls",
                "--project-dir",
                str(shop),
                "--target",
                "dev",
                "--state",
                state,
                "-s",
                "state:modified",
            ]
        )
        assert lines(r) == ["stg_orders"], r.stderr


def test_graph_takes_state(shop: Path) -> None:
    edit(shop, "models/staging/stg.yml", "select id, customer_id, amount", "select id, amount")
    r = run(
        [
            "graph",
            "--project-dir",
            str(shop),
            "--target",
            "dev",
            "--state",
            "state",
            "-s",
            "state:modified+",
        ]
    )
    assert r.exit_code == 0, r.stderr
    assert lines(r)[0] == "stg_orders"


def test_compile_takes_state(shop: Path) -> None:
    edit(shop, "models/staging/stg.yml", "select id, customer_id, amount", "select id, amount")
    r = run(
        ["compile", "--project-dir", str(shop), "--target", "ci", "--state", "state"]
        + ["-s", "state:modified", "--out", "out/manifest.json"]
    )
    assert r.exit_code == 0, r.stderr
    doc = json.loads((shop / "out" / "manifest.json").read_bytes().decode("utf-8"))
    assert doc["selectors"]["selected"] == ["model.shop.stg_orders"]


def test_state_atoms_without_state_exit_2(shop: Path) -> None:
    for command in ("ls", "graph", "compile"):
        r = run([command, "--project-dir", str(shop), "--target", "dev", "-s", "state:modified"])
        assert r.exit_code == 2 and "PLG307" in r.stderr, command
        assert r.stdout == ""


def test_result_atoms_need_run_results(shop: Path) -> None:
    for command in ("ls", "graph", "compile"):
        r = run(
            [
                command,
                "--project-dir",
                str(shop),
                "--target",
                "dev",
                "--state",
                "state",
                "-s",
                "result:fail",
            ]
        )
        assert r.exit_code == 2 and "PLG308" in r.stderr, command


def test_a_bad_state_value_is_plg305(shop: Path) -> None:
    r = run(
        [
            "ls",
            "--project-dir",
            str(shop),
            "--target",
            "dev",
            "--state",
            "state",
            "-s",
            "state:changed",
        ]
    )
    assert r.exit_code == 2 and "PLG305" in r.stderr


def test_an_unusable_state_folder_exits_1(shop: Path) -> None:
    for command in ("ls", "graph", "compile"):
        r = run([command, "--project-dir", str(shop), "--target", "dev", "--state", "nope"])
        assert r.exit_code == 1 and "PLX110" in r.stderr, command
    (shop / "state" / "manifest.json").write_text("{}", encoding="utf-8")
    r = run(["ls", "--project-dir", str(shop), "--target", "dev", "--state", "state"])
    assert r.exit_code == 1 and "PLX110" in r.stderr


def test_a_manifest_of_another_project_is_plx110(shop: Path) -> None:
    edit(shop, "pipeloom.yml", "name: shop", "name: other")
    r = run(["ls", "--project-dir", str(shop), "--target", "dev", "--state", "state"])
    assert r.exit_code == 1 and "PLX110" in r.stderr and "project 'shop'" in r.stderr


def test_result_atoms_read_the_reference_run_results(shop: Path) -> None:
    rows = [
        {"unique_id": "model.shop.stg_orders", "status": "failed"},
        {"unique_id": "model.shop.stg_payments", "status": "skipped"},
        {"unique_id": "model.shop.stg_customers", "status": "succeeded"},
    ]
    doc = {"schema": "pipeloom.run_results/1", "run": {"project": "shop"}, "results": rows}
    (shop / "state" / "run_results.json").write_text(json.dumps(doc), encoding="utf-8")
    assert modified(shop, "result:fail") == ["stg_orders"]
    assert modified(shop, "result:skip") == ["stg_payments"]
    assert modified(shop, "result:succeeded") == ["stg_customers"]
    assert modified(shop, "result:cancelled") == []


def test_unusable_run_results_are_plx110(shop: Path) -> None:
    (shop / "state" / "run_results.json").write_text("not json", encoding="utf-8")
    r = run(["ls", "--project-dir", str(shop), "--target", "dev", "--state", "state"])
    assert r.exit_code == 1 and "PLX110" in r.stderr
    doc = {"schema": "pipeloom.run_results/1", "run": {"project": "other"}, "results": []}
    (shop / "state" / "run_results.json").write_text(json.dumps(doc), encoding="utf-8")
    r = run(["ls", "--project-dir", str(shop), "--target", "dev", "--state", "state"])
    assert r.exit_code == 1 and "PLX110" in r.stderr
