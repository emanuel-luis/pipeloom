from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from pipeloom.api import Project, ProjectOptions
from pipeloom.config.resolve import ResolvedProject, merge_settings

Factory = Callable[[Mapping[str, Any]], Path]

PROJECT = """config_version: 1
name: shop
targets:
  dev:
    engine: {kind: memory}
models:
  +retries: 1
  +tags: [all, shared]
  +partition_by: [a]
  +write: {options: {x: 1, y: 1}, on_schema_change: ignore}
"""


def py(name: str, decorator: str = "@model") -> str:
    return f"from pipeloom import model, source\n\n\n{decorator}\ndef {name}():\n    return None\n"


def resolve(factory: Factory, files: Mapping[str, str], project: str = PROJECT) -> ResolvedProject:
    root = factory({"pipeloom.yml": project, **files})
    outcome = Project(ProjectOptions(project_dir=root, env={})).resolve()
    assert outcome.value is not None, outcome.diagnostics
    return outcome.value.project


def test_precedence_and_merge(project_factory: Factory) -> None:
    tree = (
        "  mart:\n    +tags: [mart, all]\n    +partition_by: [b]\n"
        "    +write: {options: {y: 2}}\n    +timeout: 60\n"
    )
    p = resolve(
        project_factory,
        {
            "models/mart/m.py": py(
                "m", '@model(tags=["own"], write={"options": {"z": 3}}, retries=2)'
            ),
            "models/other.py": py("o"),
        },
        PROJECT + tree,
    )
    m = p.models["model.shop.m"].config
    assert m.retries == 2  # the decorator beats the tree
    assert m.tags == ["all", "shared", "mart", "own"]  # ordered union, deduplicated
    assert m.partition_by == ["b"]  # lists replace
    assert m.write.options == {"x": 1, "y": 2, "z": 3}  # mappings merge deeply
    assert m.write.on_schema_change == "ignore"
    assert m.timeout == 60
    o = p.models["model.shop.o"].config
    assert (o.retries, o.partition_by) == (1, ["a"])
    assert o.tags == ["all", "shared"]  # mart settings do not leak across folders
    assert o.materialized == "table"  # built-in default


def test_tree_folders_count_from_the_model_path_root(project_factory: Factory) -> None:
    project = PROJECT.replace("name: shop", "name: shop\nmodel_paths: [models/core]")
    p = resolve(
        project_factory,
        {"models/core/m.py": py("m")},
        project + "  core:\n    +retries: 9\n  sub:\n    +retries: 7\n",
    )
    assert p.models["model.shop.m"].config.retries == 1  # `core` is the root, not a folder


def test_ids_inputs_and_locations(project_factory: Factory) -> None:
    project = PROJECT + "sources:\n  raw:\n    format: memory\n    tables:\n      r: {}\n"
    p = resolve(
        project_factory,
        {
            "models/m.py": (
                "from pipeloom import model, source\n\n\n"
                '@model(\n    retries=3,\n    inputs={"x": source("raw", "r")},\n)\n'
                "def a(x):\n    return x\n"
            ),
        },
        project,
    )
    assert list(p.sources) == ["source.shop.raw.r"]
    a = p.models["model.shop.a"]
    assert a.inputs["x"].unique_id == "source.shop.raw.r"
    assert a.inputs["x"].name == "raw.r"
    assert p.locate_model(a, "retries").line == 5
    assert p.locate_model(a, "timeout").line == 4  # not set: the decorator


def test_merge_settings_tags_ignore_null() -> None:
    cfg = merge_settings([{"tags": ["a"]}, {"tags": None}, {"tags": ["b", "a"]}])
    assert cfg.tags == ["a", "b"]


def test_project_dir_as_model_root(project_factory: Factory) -> None:
    project = PROJECT.replace("name: shop", "name: shop\nmodel_paths: ['.']")
    p = resolve(
        project_factory,
        {"m.py": py("m"), "sub/n.py": py("n")},
        project + "  sub:\n    +timeout: 30\n",
    )
    m, n = p.models["model.shop.m"].config, p.models["model.shop.n"].config
    assert (m.retries, m.timeout, m.tags) == (1, None, ["all", "shared"])
    assert (n.retries, n.timeout) == (1, 30)
