"""The per-model `catalog` setting: config cascade, compile and the filesystem layout."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.api import Project, ProjectOptions
from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.cli import main
from pipeloom.config.models import ModelConfig
from pipeloom.engine.types import Namespace, TargetRef
from tests.conftest import make_project

Factory = Callable[[Mapping[str, Any]], Path]

PROJECT = (
    "config_version: 1\nname: lake\ntargets:\n  dev:\n    engine: {kind: memory}\n"
    "    naming: {kind: none, catalog: base}\n"
)


def relations(root: Path) -> dict[str, str | None]:
    outcome = Project(ProjectOptions(project_dir=root, env={})).compile()
    assert outcome.value is not None, outcome.diagnostics
    return {
        n.name: None if n.relation is None else n.relation.relation_name
        for n in outcome.value.nodes.values()
    }


def test_config_parses_catalog() -> None:
    assert ModelConfig.model_validate({"name": "m", "sql": "x", "catalog": "c"}).catalog == "c"


def test_cascade_and_naming_fallback(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": PROJECT,
            "models/bronze/_defaults.yml": "models: {catalog: bz}\n",
            "models/bronze/b.yml": (
                "models:\n  - {name: b1, sql: select 1}\n"
                "  - {name: b2, sql: select 1, catalog: own}\n"
                "  - {name: e, sql: select 1, materialized: ephemeral}\n"
            ),
            "models/other.yml": "models:\n  - {name: o, sql: select 1}\n",
        }
    )
    assert relations(root) == {
        "b1": "bz.main.b1",  # folder default
        "b2": "own.main.b2",  # model beats folder
        "e": None,  # ephemeral: no relation
        "o": "base.main.o",  # unset: the naming option stays
    }


def test_cli_compile_manifest_relations(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": PROJECT + "defaults:\n  models: {schema: tpch}\n",
            "models/bronze/_defaults.yml": "models: {catalog: lake_bronze}\n",
            "models/gold/_defaults.yml": "models: {catalog: lake_gold}\n",
            "models/bronze/o.yml": "models:\n  - {name: orders, sql: select 1}\n",
            "models/gold/r.yml": "models:\n  - {name: revenue, sql: select 1}\n",
        },
    )
    out = tmp_path / "m.json"
    result = CliRunner().invoke(
        main, ["compile", "--project-dir", str(tmp_path), "--out", str(out)], env={}
    )
    assert result.exit_code == 0, result.output
    doc = json.loads(out.read_text(encoding="utf-8"))
    got = {n["name"]: n["relation"] for n in doc["nodes"].values()}
    assert got == {
        "orders": {"catalog": "lake_bronze", "schema": "tpch", "name": "orders"},
        "revenue": {"catalog": "lake_gold", "schema": "tpch", "name": "revenue"},
    }


def test_filesystem_catalog_layout(tmp_path: Path) -> None:
    cat = FilesystemCatalog(tmp_path)
    assert cat.table(TargetRef("c", "s", "t")).path == tmp_path / "c" / "s" / "t"
    assert cat.table(TargetRef(None, "s", "t")).path == tmp_path / "s" / "t"
    cat.ensure_namespace(Namespace("c", "s"))
    cat.ensure_namespace(Namespace(None, "z"))
    assert (tmp_path / "c" / "s").is_dir() and (tmp_path / "z").is_dir()


@pytest.mark.parametrize("bad", ["..", "a/b", ""])
def test_filesystem_catalog_rejects_a_bad_catalog(tmp_path: Path, bad: str) -> None:
    cat = FilesystemCatalog(tmp_path)
    with pytest.raises(ValueError, match="directory name"):
        cat.table(TargetRef(bad, "s", "t"))
    with pytest.raises(ValueError, match="directory name"):
        cat.ensure_namespace(Namespace(bad, "s"))


def test_project_level_default_cascades(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": PROJECT + "defaults:\n  models: {catalog: dflt}\n",
            "models/m.yml": (
                "models:\n  - {name: a, sql: select 1}\n"
                "  - {name: b, sql: select 1, catalog: own}\n"
            ),
        }
    )
    assert relations(root) == {"a": "dflt.main.a", "b": "own.main.b"}


def test_per_user_naming_keeps_the_model_catalog(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": PROJECT.replace("kind: none, catalog: base", "kind: per_user"),
            "models/m.yml": "models:\n  - {name: a, sql: select 1, catalog: gold}\n",
        }
    )
    got = relations(root)["a"]
    assert got is not None and got.startswith("gold.main_") and got.endswith(".a")


def test_empty_catalog_is_rejected(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": PROJECT,
            "models/m.yml": "models:\n  - {name: a, sql: select 1, catalog: ''}\n",
        }
    )
    out = Project(ProjectOptions(project_dir=root, env={})).validate()
    assert {d.code for d in out.diagnostics} == {"PLS102"}
