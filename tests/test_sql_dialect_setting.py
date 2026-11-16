"""The inheritable `sql_dialect` model setting."""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from pipeloom.api import Project, ProjectOptions
from pipeloom.config.models import ModelConfig
from pipeloom.plan.manifest import manifest_bytes

Factory = Callable[[Mapping[str, Any]], Path]

PROJECT = """\
config_version: 1
name: lake
targets:
  dev:
    engine:
      kind: memory
defaults:
  models:
    sql_dialect: spark
"""


def test_cascade_project_folder_model(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": PROJECT,
            "models/t/_defaults.yml": "models:\n  sql_dialect: tsql\n",
            "models/t/a.yml": "models:\n  - name: a\n    sql: select 1\n",
            "models/t/b.yml": "models:\n  - name: b\n    sql: select 1\n    sql_dialect: duckdb\n",
            "models/c.yml": "models:\n  - name: c\n    sql: select 1\n",
        }
    )
    plan = Project(ProjectOptions(project_dir=root, env={})).compile().value
    assert plan is not None
    got = {n.name: n.config.sql_dialect for n in plan.nodes.values()}
    assert got == {"a": "tsql", "b": "duckdb", "c": "spark"}


def test_unknown_dialect_is_rejected_when_sqlglot_is_installed() -> None:
    pytest.importorskip("sqlglot")
    with pytest.raises(ValidationError, match="unknown sql_dialect 'nosuch'"):
        ModelConfig.model_validate({"name": "m", "sql": "x", "sql_dialect": "nosuch"})


def test_any_dialect_is_accepted_without_sqlglot(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "sqlglot.dialects.dialect", None)
    m = ModelConfig.model_validate({"name": "m", "sql": "x", "sql_dialect": "nosuch"})
    assert m.sql_dialect == "nosuch"


def test_unset_dialect_stays_out_of_the_manifest(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": PROJECT.split("defaults:")[0],
            "models/a.yml": "models:\n  - name: a\n    sql: select 1\n",
        }
    )
    plan = Project(ProjectOptions(project_dir=root, env={})).compile().value
    assert plan is not None
    assert b"sql_dialect" not in manifest_bytes(plan)
