"""PLC507 from the catalog's capabilities."""

from __future__ import annotations

from pathlib import Path

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.filesystem import SPEC as FILESYSTEM
from pipeloom.engine.types import CatalogCapabilities
from tests.conftest import make_project

YML = """config_version: 1
name: shop
targets:
  dev:
    engine: {kind: memory}
    catalog: {kind: noviews}
"""


def test_filesystem_catalog_declares_views() -> None:
    assert FILESYSTEM.capabilities == CatalogCapabilities(views=True)


def _codes(tmp_path: Path, model: str) -> list[str]:
    make_project(tmp_path, {"pipeloom.yml": YML, "models/m.yml": f"models:\n  - {model}\n"})
    opts = ProjectOptions(project_dir=tmp_path, plugins=("tests.plugins.fakes:NOVIEWS",), env={})
    return [d.code for d in Project(opts).validate().diagnostics]


def test_view_on_a_catalog_without_views_is_plc507(tmp_path: Path) -> None:
    assert _codes(tmp_path, "{name: a, sql: select 1, materialized: view}") == ["PLC507"]


def test_table_on_a_catalog_without_views_is_fine(tmp_path: Path) -> None:
    assert _codes(tmp_path, "{name: a, sql: select 1}") == []
