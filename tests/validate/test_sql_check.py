from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions

PROJECT = {
    "pipeloom.yml": "config_version: 1\nname: shop\ntargets:\n  dev:\n    engine: {kind: memory}\n",
    "models/m.yml": (
        "sources:\n  - {name: raw, format: memory, location: raw/o}\n"
        "models:\n  - {name: a, sql: 'select * from Orders', inputs: {orders: {source: raw}}}\n"
        "  - {name: b, sql: 'select * from (', inputs: {}}\n"
    ),
}


def codes(project_factory: Callable[[Mapping[str, Any]], Path]) -> set[str]:
    options = ProjectOptions(project_dir=project_factory(PROJECT), env={})
    return {d.code for d in Project(options).validate().diagnostics}


def test_names_are_compared_case_sensitively(
    project_factory: Callable[[Mapping[str, Any]], Path],
) -> None:
    assert {"PLK603", "PLK605"} <= codes(project_factory)


def test_nothing_is_imported_or_reported_without_sqlglot(
    project_factory: Callable[[Mapping[str, Any]], Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("importlib.util.find_spec", lambda name, *a, **k: None)
    monkeypatch.setitem(sys.modules, "sqlglot", None)  # an import would raise ImportError
    assert not {"PLK603", "PLK605"} & codes(project_factory)
