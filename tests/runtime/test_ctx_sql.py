"""ctx.sql, keyword calls and Python views on legacy YAML-declared Python models (memory)."""

from __future__ import annotations

import functools
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.plan.model import PythonBody
from pipeloom.runtime.context import ModelContext
from pipeloom.runtime.run import RunOptions
from pipeloom.runtime.runner import ModelRunner
from pipeloom.runtime.sql import SqlglotMissingError, SqlUnsupportedError, translate
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project

PROJECT = """\
config_version: 1
name: shop
python_paths: [src]
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
"""

MODELS = """\
models:
  - name: src
    python: "m:src"
  - name: out
    python: "m:{fn}"
    materialized: {materialized}
    inputs:
      src:
        ref: src
  - name: check
    python: "m:check"
    inputs:
      out:
        ref: out
"""

CODE = """\
def src(ctx):
    return [{"id": 1, "label": "a"}, {"id": 2, "label": "b"}, {"id": 3, "label": None}]

def check(ctx, out):
    got = sorted(r["id"] for r in out)
    assert got == EXPECTED, got
    return out

EXPECTED = [2, 3]

def plain(ctx, src):
    return ctx.sql("select id from src where id > 1")

def extra(ctx, src):
    return ctx.sql("select id from more", inputs={"more": [{"id": 2}, {"id": 3}]})

def replace(ctx, src):
    return ctx.sql("select id from src", inputs={"src": [{"id": 2}, {"id": 3}]})

def tsql(ctx, src):
    return ctx.sql("select top 2 id from src order by id desc", dialect="tsql")

def view(ctx, src):
    return ctx.sql("select id from src where id > 1")

def view_misuse(ctx, src):
    return ctx.sql(f"select id from src limit {len(src)}")

def view_fstring(ctx, src):
    return ctx.sql(f"select id from {src}")

def view_frames(ctx, src):
    return ctx.sql("select id from more", inputs={"more": [{"id": 1}]})
"""


def run(tmp_path: Path, fn: str, materialized: str = "table") -> list[tuple[str, str]]:
    """Run the project; the (code, message) of each failed unit."""
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": PROJECT,
            "models/m.yml": MODELS.format(fn=fn, materialized=materialized),
            "src/m.py": CODE,
        },
    )
    project = Project(ProjectOptions(project_dir=root, env={}), clock=FakeClock())
    outcome = project.run(RunOptions(run_id="r", workers=1))
    return [(d.code, d.message) for d in outcome.diagnostics if d.code.startswith("PL")]


@pytest.mark.parametrize("fn", ["plain", "extra", "replace"])
def test_ctx_sql_and_keyword_calls(tmp_path: Path, fn: str) -> None:
    assert run(tmp_path, fn) == []


def test_ctx_is_passed_only_when_it_is_a_parameter() -> None:
    """Legacy PLK602 still asks for ctx first; decorated models may leave it out."""

    class Loader:
        def __init__(self, fn: Callable[..., Any]) -> None:
            self.load = lambda ref: fn

    def no_ctx(*, src: int) -> int:
        return src

    def with_ctx(src: int, ctx: str) -> tuple[str, int]:
        return ctx, src

    runner = ModelRunner.__new__(ModelRunner)
    body = PythonBody(callable="m:f", file="src/m.py")
    ctx = cast(ModelContext, "ctx")
    runner._loader = Loader(no_ctx)  # type: ignore[assignment]
    assert runner._call(body, ctx, {"src": 1}) == 1
    runner._loader = Loader(with_ctx)  # type: ignore[assignment]
    assert runner._call(body, ctx, {"src": 1}) == ("ctx", 1)


def test_ctx_goes_first_and_positional_when_it_is_the_first_parameter() -> None:
    class Loader:
        def __init__(self, fn: Callable[..., Any]) -> None:
            self.load = lambda ref: fn

    def positional_only(ctx: str, /, src: int) -> tuple[str, int]:
        return ctx, src

    def inner(ctx: str, src: int) -> tuple[str, int]:
        return ctx, src

    @functools.wraps(inner)
    def wrapper(*args: Any, **kwargs: Any) -> tuple[str, int]:
        return inner(*args, **kwargs)

    runner = ModelRunner.__new__(ModelRunner)
    body = PythonBody(callable="m:f", file="src/m.py")
    ctx = cast(ModelContext, "ctx")

    def plain_wrapper(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
        return args[0], kwargs["src"]

    for fn in (positional_only, wrapper, plain_wrapper):
        runner._loader = Loader(fn)  # type: ignore[assignment]
        assert runner._call(body, ctx, {"src": 1}) == ("ctx", 1)


def test_a_declared_dialect_is_translated(tmp_path: Path) -> None:
    pytest.importorskip("sqlglot")
    assert run(tmp_path, "tsql") == []


def test_a_python_view(tmp_path: Path) -> None:
    assert run(tmp_path, "view", "view") == []


@pytest.mark.parametrize(
    ("fn", "needle"),
    [
        ("view_misuse", "used input 'src' as a frame"),
        ("view_fstring", "used input 'src' as a frame"),
        ("view_frames", "passed inputs="),
    ],
)
def test_view_mode_misuse_is_plx120(tmp_path: Path, fn: str, needle: str) -> None:
    found = run(tmp_path, fn, "view")
    assert [c for c, _ in found if c.startswith("PLX")] == ["PLX120"], found
    assert any(needle in m for _, m in found), found


def test_translate_needs_sqlglot_only_for_another_dialect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(__import__("sys").modules, "sqlglot", None)
    assert translate("select 1", None, "sqlite") == "select 1"
    assert translate("select 1", "sqlite", "sqlite") == "select 1"
    with pytest.raises(SqlglotMissingError, match="needs sqlglot"):
        translate("select 1", "spark", "sqlite")
    with pytest.raises(SqlUnsupportedError):
        translate("select 1", None, None)


def test_ctx_sql_outside_a_run_raises() -> None:
    ctx = ModelContext.__new__(ModelContext)
    object.__setattr__(ctx, "_run_sql", None)
    with pytest.raises(RuntimeError, match="only while pipeloom runs"):
        ctx.sql("select 1")
