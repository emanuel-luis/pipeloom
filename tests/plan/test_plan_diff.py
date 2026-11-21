"""`plan --diff` and its change categories (ADR-0052)."""

from __future__ import annotations

import importlib.util
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.validate.static import sql_change
from tests.conftest import make_project

YML = """config_version: 1
name: shop
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
"""
A = """from pipeloom import model


@model(columns=[{"name": "x", "dtype": "int64"}, {"name": "y", "dtype": "int64"}])
def a(ctx):
    return ctx.sql("select 1 as x, 2 as y")
"""
B = """from pipeloom import model


@model
def b(ctx, a):
    return ctx.sql("select x, y from a")
"""
C = """from pipeloom import model


@model
def c(ctx, b):
    return b
"""
D = """from pipeloom import model


@model
def d(ctx):
    return ctx.sql("select 1 as z")
"""
E = """from pipeloom import model


@model
def e(ctx, d):
    return d
"""


def run(root: Path, *args: str) -> Any:
    return CliRunner().invoke(main, [*args, "--project-dir", str(root)], env={})


@pytest.fixture
def root(tmp_path: Path) -> Path:
    files = {"pipeloom.yml": YML, "models/a.py": A, "models/b.py": B, "models/c.py": C}
    root = make_project(tmp_path, {**files, "models/d.py": D}, convert=False)
    done = run(root, "compile", "--stable", "--out", "state/manifest.json")
    assert done.exit_code == 0, done.stderr
    return root


def edit(root: Path, model: str, old: str, new: str) -> None:
    path = root / f"models/{model}.py"
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new), encoding="utf-8")


def diff(root: Path, *extra: str, code: int | None = None) -> dict[str, tuple[str, str]]:
    """Category and reason by model name, from the JSON output."""
    done = run(root, "plan", "--diff", "--state", "state", "--output", "json", *extra)
    assert done.exit_code in ((0, 1) if code is None else (code,)), done.stderr
    doc = json.loads(done.stdout)
    return {c["name"]: (c["category"], c["reason"]) for c in doc["changes"]}


def categories(root: Path, *extra: str) -> dict[str, str]:
    return {name: cat for name, (cat, _) in diff(root, *extra).items()}


def test_an_unchanged_project_has_no_changes(root: Path) -> None:
    done = run(root, "plan", "--diff", "--state", "state")
    assert done.exit_code == 0, done.stderr
    assert done.stdout == "no changes\n"


def test_layout_and_comments_are_no_change(root: Path) -> None:
    edit(root, "b", '"select x, y from a"', '"select x,\\n  y -- both\\n from a"')
    edit(root, "d", "def d(ctx):\n", "def d(ctx):\n    # a comment\n")
    assert diff(root, code=0) == {}


def test_new_and_removed(root: Path) -> None:
    (root / "models/d.py").unlink()
    (root / "models/e.py").write_text(D.replace("def d", "def e"), encoding="utf-8")
    assert categories(root) == {"d": "removed", "e": "new"}
    assert run(root, "plan", "--diff", "--state", "state").exit_code == 1  # removed is breaking


def test_adding_a_projected_column_is_non_breaking(root: Path) -> None:
    edit(root, "b", "select x, y from a", "select x, y, x + 1 as z from a")
    assert diff(root, code=0) == {
        "b": ("non-breaking", "added z"),
        "c": ("indirect-non-breaking", "downstream of b"),
    }


def test_a_filter_is_breaking_and_reaches_descendants(root: Path) -> None:
    edit(root, "b", "select x, y from a", "select x, y from a where y > 0")
    assert diff(root, code=1) == {
        "b": ("breaking", "the query changed beyond the added columns"),
        "c": ("indirect-breaking", "downstream of b"),
    }


@pytest.mark.parametrize(
    ("new", "why"),
    [
        ("select x from a", "an existing column changed or was removed"),
        ("select x, y, count(*) as n from a", "added column 'n' may change the row count"),
        ("select x, y, (select 1) as s from a", "added column 's' may change the row count"),
        (
            "select x, y, generate_series(1, 3) as g from a",
            "added column 'g' may change the row count",
        ),
        ("select x, y, x as y from a", "an added column has no name or reuses one"),
        ("select x, y from a union all select x, y from a", "the query changed"),
        ("select from from", "the SQL cannot be parsed"),
        ("select x, y from a; select 1", "the SQL is not one statement"),
    ],
)
def test_sql_changes_that_are_breaking(root: Path, new: str, why: str) -> None:
    edit(root, "b", "select x, y from a", new)
    assert diff(root, code=1)["b"] == ("breaking", why)


def test_sql_change_rules() -> None:
    assert sql_change("select a from t", "SELECT  a FROM t", None) == ("same", "")
    assert sql_change("select * from t", "select *, 1 as k from t", None)[0] == "breaking"
    assert sql_change("select distinct a from t", "select distinct a, b from t", None)[0] == (
        "breaking"
    )
    assert (
        sql_change(
            "select a, sum(b) as s from t group by a",
            "select a, sum(b) as s, max(b) as m from t group by a",
            None,
        )[0]
        == "breaking"
    )
    assert sql_change("select a from t", "select a, explode(b) as e from t", "spark")[0] == (
        "breaking"
    )
    assert sql_change("select a from t", "select a, sum(b) over () as s from t", None) == (
        "non-breaking",
        "added s",
    )


def test_python_frame_code_is_breaking(root: Path) -> None:
    edit(root, "c", "    return b\n", "    return b.head(1)\n")
    assert diff(root, code=1) == {"c": ("breaking", "Python code changed")}


def test_python_around_the_sql_is_breaking(root: Path) -> None:
    edit(
        root,
        "b",
        '    return ctx.sql("select x, y from a")',
        '    q = 1\n    return ctx.sql("select x, y from a")',
    )
    assert diff(root, code=1)["b"] == ("breaking", "Python code around the SQL changed")


def test_missing_sqlglot_is_breaking(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real = importlib.util.find_spec

    def find_spec(name: str, *args: Any) -> Any:
        return None if name == "sqlglot" else real(name, *args)

    monkeypatch.setattr("pipeloom.validate.static.importlib.util.find_spec", find_spec)
    edit(root, "b", "select x, y from a", "select x, y, x + 1 as z from a")
    reason = "sqlglot is not installed, so the SQL change is unknown"
    assert diff(root, code=1)["b"] == ("breaking", reason)


def test_dropping_a_contract_column_exits_1_and_marks_descendants(root: Path) -> None:
    edit(root, "a", ', {"name": "y", "dtype": "int64"}', "")
    edit(root, "a", "select 1 as x, 2 as y", "select 1 as x")
    assert diff(root, code=1) == {
        "a": ("breaking", "an existing column changed or was removed; column 'y' dropped"),
        "b": ("indirect-breaking", "downstream of a"),
        "c": ("indirect-breaking", "downstream of a"),
    }


@pytest.mark.parametrize(
    ("column", "category"),
    [
        ('{"name": "w", "dtype": "string"}', "non-breaking"),
        ('{"name": "w", "dtype": "string", "nullable": False}', "breaking"),
    ],
)
def test_adding_a_contract_column(root: Path, column: str, category: str) -> None:
    edit(root, "a", '"int64"}])', f'"int64"}}, {column}])')
    assert categories(root)["a"] == category


def test_retyping_a_contract_column_is_breaking(root: Path) -> None:
    edit(root, "a", '{"name": "y", "dtype": "int64"}', '{"name": "y", "dtype": "string"}')
    assert diff(root)["a"] == ("breaking", "column 'y' retyped")


@pytest.mark.parametrize(
    ("decorator", "category", "why"),
    [
        (
            '@model(tags=["x"], meta={"o": 1})',
            "metadata",
            "config 'meta' changed; config 'tags' changed",
        ),
        ("@model(retries=2)", "metadata", "config 'retries' changed"),
        ('@model(cluster_by=["z"])', "non-breaking", "config 'cluster_by' changed"),
        (
            '@model(materialized="view")',
            "breaking",
            "config 'materialized' changed; config 'write' changed",
        ),
        ('@model(partition_by=["z"])', "breaking", "config 'partition_by' changed"),
        ('@model(schema="other")', "breaking", "config 'schema' changed"),
        (
            '@model(expectations=[{"name": "p", "expr": "z > 0", "on_violation": "warn"}])',
            "non-breaking",
            "expectations changed, none drops rows",
        ),
        (
            '@model(expectations=[{"name": "p", "expr": "z > 0", "on_violation": "drop"}])',
            "breaking",
            "an expectation that drops rows changed",
        ),
        (
            '@model(data_tests=[{"name": "u", "check": {"unique": ["z"]}}])',
            "metadata",
            "data tests changed",
        ),
    ],
)
def test_config_and_tests(root: Path, decorator: str, category: str, why: str) -> None:
    edit(root, "d", "@model\n", decorator + "\n")
    assert diff(root)["d"] == (category, why)


def test_own_breaking_beats_indirect_non_breaking(root: Path) -> None:
    edit(root, "a", "select 1 as x, 2 as y", "select 1 as x, 2 as y, 3 as w")
    edit(root, "b", "select x, y from a", "select x from a")
    got = categories(root)
    assert got == {"a": "non-breaking", "b": "breaking", "c": "indirect-breaking"}


def test_a_reference_without_split_checksums_is_breaking(root: Path) -> None:
    path = root / "state/manifest.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    for node in doc["nodes"].values():
        for key in ("checksums", "checksum_version", "sql"):
            node.pop(key, None)
    path.write_text(json.dumps(doc), encoding="utf-8")
    edit(root, "b", "select x, y from a", "select x, y, x + 1 as z from a")
    done = run(root, "plan", "--diff", "--state", "state")
    assert done.exit_code == 1
    assert "PLG309" in done.stderr
    assert "legacy" in done.stdout
    assert categories(root) == {"b": "breaking", "c": "indirect-breaking"}


def test_a_reference_without_sql_records_is_breaking_for_code(root: Path) -> None:
    path = root / "state/manifest.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    for node in doc["nodes"].values():
        node.pop("sql", None)
    path.write_text(json.dumps(doc), encoding="utf-8")
    edit(root, "b", "select x, y from a", "select x, y, x + 1 as z from a")
    reason = "code changed and the reference has no SQL record"
    assert diff(root, code=1)["b"] == ("breaking", reason)


def test_fail_on(root: Path) -> None:
    edit(root, "d", "@model\n", '@model(tags=["x"])\n')
    base = ["plan", "--diff", "--state", "state"]
    assert run(root, *base).exit_code == 0
    assert run(root, *base, "--fail-on", "breaking").exit_code == 0
    assert run(root, *base, "--fail-on", "any").exit_code == 1
    edit(root, "d", "select 1 as z", "select 2 as z")
    assert run(root, *base, "--fail-on", "none").exit_code == 0
    assert run(root, *base, "--fail-on", "bad").exit_code == 2


def test_select_and_exclude_filter_the_report(root: Path) -> None:
    (root / "models/d.py").unlink()
    edit(root, "b", "select x, y from a", "select x from a")
    assert categories(root, "--select", "c") == {"c": "indirect-breaking"}
    assert categories(root, "--exclude", "c") == {"b": "breaking"}
    assert categories(root, "-s", "state:modified+") == {"b": "breaking", "c": "indirect-breaking"}
    assert run(root, "plan", "--diff", "--state", "state", "-s", "bad:x").exit_code == 2


def test_usage_errors(root: Path) -> None:
    assert run(root, "plan", "--state", "state").exit_code == 2
    assert run(root, "plan", "--diff").exit_code == 2
    assert run(root, "plan", "--diff", "--state", "state", "--output", "xml").exit_code == 2


def test_a_missing_state_folder_exits_1(root: Path) -> None:
    done = run(root, "plan", "--diff", "--state", "nowhere")
    assert done.exit_code == 1 and "PLX110" in done.stderr


def test_other_shared_flags(root: Path) -> None:
    done = run(
        root, "plan", "--diff", "--state", "state", "--target", "dev", "--log-format", "json"
    )
    assert done.exit_code == 0, done.stderr
    assert done.stdout == "no changes\n"
    done = run(root, "plan", "--diff", "--state", "state", "--vars", "k: 1", "--plugin", "x")
    assert done.exit_code in (0, 1)


def test_golden_outputs(root: Path, golden: Callable[[str, str], None]) -> None:
    (root / "models/d.py").unlink()
    (root / "models/e.py").write_text(D.replace("def d", "def e"), encoding="utf-8")
    edit(root, "a", "select 1 as x, 2 as y", "select 1 as x, 2 as y, 3 as w")
    edit(root, "c", "    return b\n", "    return b.head(1)\n")
    text = run(root, "plan", "--diff", "--state", "state")
    assert text.exit_code == 1
    golden("plan_diff/text.txt", text.stdout)
    js = run(root, "plan", "--diff", "--state", "state", "--output", "json", "--fail-on", "any")
    assert js.exit_code == 1
    golden("plan_diff/plan_diff.json", js.stdout)


TWO = """from pipeloom import model


def helper(ctx):
    return ctx.sql("select 1 as h")


@model
def p(ctx):
    return ctx.sql("select 1 as x")


@model
def q(ctx):
    return ctx.sql("select 2 as y")
"""


def test_a_sibling_query_is_not_a_change_but_a_helper_query_is(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/two.py": TWO}, convert=False)
    assert run(root, "compile", "--stable", "--out", "state/manifest.json").exit_code == 0
    edit(root, "two", "select 1 as x", "select 1 as x, 3 as z")
    assert categories(root) == {"p": "non-breaking"}
    edit(root, "two", "select 1 as h", "select 2 as h")
    assert diff(root, code=1) == {
        "p": ("breaking", "Python code around the SQL changed"),
        "q": ("breaking", "Python code around the SQL changed"),
    }


FEEDS = """from pipeloom import model


@model
def m(ctx):
    k = ctx.sql("select 1 as k")
    return ctx.sql("select distinct * from k", inputs={"k": k})


@model
def n(ctx):
    return ctx.sql("select 1 as k").unique()
"""


def test_a_query_that_is_not_the_returned_value_is_breaking(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/f.py": FEEDS}, convert=False)
    assert run(root, "compile", "--stable", "--out", "state/manifest.json").exit_code == 0
    edit(root, "f", '"select 1 as k")\n', '"select 1 as k, 2 as v")\n')
    edit(root, "f", '"select 1 as k").unique', '"select 1 as k, 2 as v").unique')
    why = "the changed query is not the value the model returns"
    assert diff(root, code=1) == {"m": ("breaking", why), "n": ("breaking", why)}


CALLS = """from pipeloom import model


@model
def p(ctx):
    return ctx.sql("select 1 as x")


@model
def r(ctx):
    return p(ctx)
"""


def test_a_caller_of_a_model_function_sees_its_sql_change(tmp_path: Path) -> None:
    root = make_project(tmp_path, {"pipeloom.yml": YML, "models/c.py": CALLS}, convert=False)
    assert run(root, "compile", "--stable", "--out", "state/manifest.json").exit_code == 0
    edit(root, "c", "select 1 as x", "select 1 as x where 1 = 0")
    got = categories(root)
    assert got == {"p": "breaking", "r": "breaking"}


def test_an_added_alias_that_the_query_reads_is_breaking() -> None:
    old, new = "select a from t order by b limit 10", "select a, c as b from t order by b limit 10"
    assert sql_change(old, new, "duckdb") == (
        "breaking",
        "added column 'b' shadows a name the query uses",
    )
    assert (
        sql_change("select a from t where b > 0", "select a, c as b from t where b > 0", None)[0]
        == "breaking"
    )
    assert sql_change("select a from t", "select a, b as x from t", None)[0] == "non-breaking"
