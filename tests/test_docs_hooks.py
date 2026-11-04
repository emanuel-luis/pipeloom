from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

from pipeloom.config.schema import dump_schema
from pipeloom.diagnostics.codes import CATALOG

ROOT = Path(__file__).parent.parent


def load_hooks() -> ModuleType:
    spec = importlib.util.spec_from_file_location("docs_hooks", ROOT / "tools" / "docs_hooks.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generated_pages() -> None:
    pages = load_hooks().generated_pages()
    assert sorted(pages) == [
        "reference/errors.md",
        "schema/v1/defaults.json",
        "schema/v1/models.json",
        "schema/v1/project.json",
    ]
    for code in CATALOG:
        assert f"### {code} {{#{code.lower()}}}" in pages["reference/errors.md"]
    project = json.loads(pages["schema/v1/project.json"])
    assert project["$id"].endswith("/latest/schema/v1/project.json")
    assert pages["schema/v1/project.json"] == dump_schema(project)


def test_on_files_appends_generated_files() -> None:
    pytest.importorskip("mkdocs")  # the docs dependency group is not always installed
    from mkdocs.config.defaults import MkDocsConfig
    from mkdocs.structure.files import Files

    config = MkDocsConfig()
    config.load_dict({"site_name": "x", "docs_dir": str(ROOT / "docs"), "plugins": []})
    config.validate()
    config.plugins._current_plugin = None  # normally set while a plugin event runs
    files = load_hooks().on_files(Files([]), config)
    paths = sorted(f.src_uri for f in files)
    assert paths == [
        "reference/errors.md",
        "schema/v1/defaults.json",
        "schema/v1/models.json",
        "schema/v1/project.json",
    ]
