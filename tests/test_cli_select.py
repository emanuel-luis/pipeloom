from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from tests.conftest import make_project

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


def test_validate_ok_on_graph_project(project: list[str]) -> None:
    r = run(["validate", *project])
    assert r.exit_code == 0
    assert r.stdout.strip() == "Project 'shop' is valid."


def test_ls_events_go_to_stderr(project: list[str]) -> None:
    r = run(["ls", *project, "--log-format", "json"])
    types = [json.loads(line)["type"] for line in r.stderr.splitlines()]
    assert types[0] == "invocation.started" and types[-1] == "invocation.completed"
