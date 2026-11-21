"""Split checksums and `state:modified.<part>` (ADR-0052)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.reference import load_reference
from pipeloom.cli import main
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.graph.selectors import Selection, SelectorError, apply
from pipeloom.plan.checksums import code_digest, normalize_sql
from pipeloom.plan.compile import model_checksums, split_checksums
from tests.conftest import make_project

YML = """config_version: 1
name: shop
vars:
  region: eu
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
models:
  +meta:
    region: ${var:region}
    owner: ${env:OWNER:-nobody}
"""
MODELS = '''from pipeloom import model


@model(data_tests=[{"name": "x_unique", "check": {"unique": ["x"]}}])
def a(ctx):
    """Docstring."""
    return ctx.sql("select 1 as x")


@model
def b(ctx, a):
    return a
'''
B_ONLY = "\n\n@model\ndef b(ctx, a):\n    return a\n"


def run(root: Path, *args: str, env: dict[str, str] | None = None) -> Any:
    return CliRunner().invoke(main, [*args, "--project-dir", str(root)], env=env or {})


@pytest.fixture
def root(tmp_path: Path) -> Path:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/m.py": MODELS}, convert=False)
    done = run(root, "compile", "--stable", "--out", "state/manifest.json")
    assert done.exit_code == 0, done.stderr
    return root


def why(root: Path, *extra: str, env: dict[str, str] | None = None) -> list[str]:
    done = run(root, "ls", "--state", "state", "--why", *extra, env=env)
    assert done.exit_code == 0, done.stderr
    return done.stdout.splitlines()


def chosen(root: Path, expr: str) -> list[str]:
    done = run(root, "ls", "--state", "state", "-s", expr)
    assert done.exit_code == 0, done.stderr
    return done.stdout.splitlines()


def edit(root: Path, old: str, new: str) -> None:
    path = root / "models/m.py"
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new), encoding="utf-8")


def test_an_unchanged_project_is_unchanged(root: Path) -> None:
    assert why(root) == ["a: unchanged", "b: unchanged"]


def test_layout_comments_and_docstrings_are_not_code(root: Path) -> None:
    edit(root, '    """Docstring."""\n', '    # a comment\n\n    """Other text."""\n')
    edit(root, '"select 1 as x"', '"select  1 -- one\\n as x /* x */ "')
    assert chosen(root, "state:modified.code") == []
    assert chosen(root, "state:modified") == ["a", "b"]
    assert why(root) == ["a: legacy only", "b: legacy only"]


def test_a_code_change_reaches_children_as_upstream(root: Path) -> None:
    edit(root, "select 1 as x", "select 2 as x")
    assert why(root) == ["a: code", "b: code, upstream"]
    assert chosen(root, "state:modified.upstream") == ["b"]


def test_editing_a_data_test_selects_only_tests(root: Path) -> None:
    edit(root, '"unique": ["x"]', '"not_null": ["x"]')
    assert chosen(root, "state:modified.tests") == ["a"]
    for part in ("code", "config", "contract", "upstream"):
        assert chosen(root, f"state:modified.{part}") == []
    assert why(root, "-s", "a") == ["a: tests"]


def test_config_hashes_vars_but_not_env_values(root: Path) -> None:
    assert why(root, env={"OWNER": "s3cret"}) == ["a: legacy only", "b: legacy only"]
    assert "s3cret" not in (root / "state/manifest.json").read_text(encoding="utf-8")
    done = run(root, "ls", "--state", "state", "-s", "state:modified", "--vars", "region: us")
    assert done.stdout.splitlines() == ["a", "b"]
    out = run(root, "ls", "--state", "state", "--why", "--vars", "region: us")
    assert out.stdout.splitlines() == ["a: config", "b: config, upstream"]


def test_the_plan_hash_leaves_split_checksums_out(root: Path) -> None:
    doc = json.loads((root / "state/manifest.json").read_text(encoding="utf-8"))
    node = doc["nodes"]["model.shop.a"]
    assert node["checksum_version"] == 1
    assert sorted(node["checksums"]) == ["code", "config", "contract", "tests", "upstream"]


def test_why_needs_state_and_text_output(root: Path) -> None:
    assert run(root, "ls", "--why").exit_code == 2
    done = run(root, "ls", "--state", "state", "--why", "--output", "json")
    assert done.exit_code == 2 and "text output" in done.stderr


def test_sql_and_code_normalization() -> None:
    assert normalize_sql(" select 'a  -- b' ,\n x -- c\n/* d */ from t ") == (
        "select 'a  -- b' , x from t"
    )
    one = code_digest(b'def f(ctx):\n    """Doc."""\n    return ctx.sql("select 1")\n', "m:f")
    two = code_digest(b"def f(ctx):  # c\n\n    return ctx.sql('select   1')\n", "m:f")
    assert one == two != code_digest(b"def f(ctx):\n    return ctx.sql('select 2')\n", "m:f")


def test_an_f_string_edit_is_a_code_change() -> None:
    def src(x: int) -> bytes:
        sql = f"select 1 from t where d = '{{d}}' AND s = 'a -- b' AND x = {x}"
        return f'def f(ctx, d):\n    return ctx.sql(f"{sql}")\n'.encode()

    assert code_digest(src(1), "m:f") != code_digest(src(2), "m:f")


@pytest.mark.parametrize(
    "text",
    [
        r"SELECT 'it\'s -- x' AS a, 1 AS b",
        "SELECT $q$ -- x $q$ AS a, 1 AS b",
        "SELECT 'open -- x\n AS a, 1 AS b",
        "SELECT 1 -- don't\n AS b",
    ],
    ids=["backslash", "dollar", "unbalanced", "quote-in-comment"],
)
def test_sql_that_cannot_be_read_safely_is_left_as_is(text: str) -> None:
    assert normalize_sql(text) == text


def test_non_finite_floats_do_not_abort_the_digest() -> None:
    assert code_digest(b"X = 1e999\n", "m:f") != code_digest(b"X = -1e999\n", "m:f")


TAGS_YML = """config_version: 1
name: shop
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
models:
  +tags: [base, "${env:TEAM:-core}"]
  marts:
    +tags: [mart]
"""


@pytest.mark.parametrize("where", ["lower", "higher"])
def test_env_in_tags_keeps_every_layer_as_written(tmp_path: Path, where: str) -> None:
    yml = TAGS_YML
    if where == "higher":
        yml = yml.replace('[base, "${env:TEAM:-core}"]', "[base]").replace(
            "[mart]", '[mart, "${env:TEAM:-core}"]'
        )
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": yml,
            "models/marts/m.py": "from pipeloom import model\n\n\n"
            "@model\ndef a(ctx):\n    return ctx.sql('select 1 as x')\n",
        },
        convert=False,
    )
    done = run(root, "compile", "--stable", "--out", "state/manifest.json")
    assert done.exit_code == 0, done.stderr
    assert why(root, env={"TEAM": "other"}) == ["a: legacy only"]
    lower, upper = ("base", "mart") if where == "lower" else ("mart", "base")
    root.joinpath("pipeloom.yml").write_text(yml.replace(f"[{upper}", "[changed"), "utf-8")
    assert why(root) == ["a: config"]
    root.joinpath("pipeloom.yml").write_text(yml.replace(f"[{lower}", "[changed"), "utf-8")
    assert why(root) == ["a: config"]


def _strip(manifest: Path) -> None:
    doc = json.loads(manifest.read_text(encoding="utf-8"))
    for node in doc["nodes"].values():
        del node["checksums"], node["checksum_version"]
    manifest.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")


def test_a_1_0_reference_falls_back_with_plg309(root: Path) -> None:
    _strip(root / "state/manifest.json")
    edit(root, "select 1 as x", "select  1 as x")
    done = run(root, "ls", "--state", "state", "-s", "state:modified.tests")
    assert done.exit_code == 0, done.stderr
    assert done.stdout.splitlines() == ["a", "b"]
    assert "PLG309" in done.stderr
    assert why(root) == ["a: legacy", "b: legacy"]


def plg309(project_dir: Path) -> list[Diagnostic]:
    """Case scenario: a 1.0 reference (no split checksums) under `state:modified.code`."""
    done = run(project_dir, "compile", "--stable", "--out", "state/manifest.json")
    assert done.exit_code == 0, done.stderr
    _strip(project_dir / "state/manifest.json")
    opts = ProjectOptions(project_dir=project_dir, env={})
    pg = Project(opts).resolve().value
    assert pg is not None
    report = Report()
    ref = load_reference(project_dir / "state", pg.project.name, report, project_dir)
    assert ref is not None
    sel = Selection(("state:modified.code",), (), ref.selector_state())
    try:
        apply(sel, pg.graph, pg.project, report, model_checksums(pg), split_checksums(pg))
    except SelectorError as err:
        report.extend([err.diagnostic])
    return report.sorted()
