from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from tests.conftest import make_project

from pipeloom.api import Project, ProjectOptions

PROJECT = {
    "pipeloom.yml": "config_version: 1\nname: shop\ntargets:\n  dev:\n    engine: {kind: memory}\n",
    "models/m.yml": (
        "sources:\n  - {name: raw, format: memory, location: raw/o}\n"
        "models:\n  - {name: a, sql: 'select * from Orders', inputs: {orders: {source: raw}}}\n"
        "  - {name: b, sql: 'select * from (', inputs: {}}\n"
    ),
}


def codes(
    project_factory: Callable[[Mapping[str, Any]], Path], files: Mapping[str, Any] = PROJECT
) -> set[str]:
    options = ProjectOptions(project_dir=project_factory(files), env={})
    return {d.code for d in Project(options).validate().diagnostics}


def test_names_are_compared_case_sensitively(
    project_factory: Callable[[Mapping[str, Any]], Path],
) -> None:
    assert {"PLK603", "PLK605"} <= codes(project_factory)


def decorated(model_args: str, call_args: str = "") -> dict[str, Any]:
    """A project whose decorated model runs `select top 2`, which only tsql parses."""
    code = (
        "from pipeloom import model\n\n\n"
        f"@model{model_args}\n"
        f'def a(ctx):\n    return ctx.sql("select top 2 x from t"{call_args})\n'
    )
    return {"pipeloom.yml": PROJECT["pipeloom.yml"], "models/m.py": code}


def test_a_decorated_model_dialect_is_the_static_parse_dialect(
    project_factory: Callable[[Mapping[str, Any]], Path],
) -> None:
    pytest.importorskip("sqlglot")
    assert "PLK605" in codes(project_factory, decorated(""))  # the engine's dialect cannot
    assert "PLK605" not in codes(project_factory, decorated('(sql_dialect="tsql")'))


def test_a_call_dialect_wins_over_the_model_dialect_and_an_unknown_one_is_plk605(
    tmp_path: Path,
) -> None:
    pytest.importorskip("sqlglot")
    ok = make_project(tmp_path / "ok", decorated('(sql_dialect="duckdb")', ', dialect="tsql"'))
    bad = make_project(tmp_path / "bad", decorated("", ', dialect="nope"'))
    for root, expected in ((ok, False), (bad, True)):
        found = Project(ProjectOptions(project_dir=root, env={})).validate().diagnostics
        assert ("PLK605" in {d.code for d in found}) is expected


def test_nothing_is_imported_or_reported_without_sqlglot(
    project_factory: Callable[[Mapping[str, Any]], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("importlib.util.find_spec", lambda name, *a, **k: None)
    monkeypatch.setitem(sys.modules, "sqlglot", None)  # an import would raise ImportError
    assert not {"PLK603", "PLK605"} & codes(project_factory)
