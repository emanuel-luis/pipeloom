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
defaults:
  models:
    retries: 1
    tags: [all, shared]
    partition_by: [a]
    write: {options: {x: 1, y: 1}, on_schema_change: ignore}
"""


def resolve(factory: Factory, files: Mapping[str, str]) -> ResolvedProject:
    root = factory({"pipeloom.yml": PROJECT, **files})
    outcome = Project(ProjectOptions(project_dir=root, env={})).resolve()
    assert outcome.value is not None, outcome.diagnostics
    return outcome.value.project


def test_precedence_and_merge(project_factory: Factory) -> None:
    p = resolve(
        project_factory,
        {
            "models/_defaults.yml": "models: {retries: 2, tags: [root], timeout: 60}\n",
            "models/mart/_defaults.yml": (
                "models: {tags: [mart, all], partition_by: [b], write: {options: {y: 2}}}\n"
            ),
            "models/mart/m.yml": (
                "models:\n  - name: m\n    sql: select 1\n    tags: [own]\n"
                "    write: {options: {z: 3}}\n    timeout: null\n"
            ),
            "models/other.yml": "models:\n  - {name: o, sql: select 1}\n",
        },
    )
    m = p.models["model.shop.m"].config
    assert m.retries == 2  # folder default beats project default
    assert m.tags == ["all", "shared", "root", "mart", "own"]  # ordered union, deduplicated
    assert m.partition_by == ["b"]  # lists replace
    assert m.write.options == {"x": 1, "y": 2, "z": 3}  # mappings merge deeply
    assert m.write.on_schema_change == "ignore"
    assert m.timeout is None  # explicit null masks the 60 below
    o = p.models["model.shop.o"].config
    assert (o.retries, o.timeout, o.partition_by) == (2, 60, ["a"])
    assert o.tags == ["all", "shared", "root"]  # mart defaults do not leak across folders
    assert o.materialized == "table"  # built-in default


def test_defaults_above_model_path_root_are_not_used(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": PROJECT.replace("name: shop", "name: shop\nmodel_paths: [models/core]"),
            "models/_defaults.yml": "models: {retries: 9}\n",
            "models/core/m.yml": "models:\n  - {name: m, sql: select 1}\n",
        }
    )
    value = Project(ProjectOptions(project_dir=root, env={})).resolve().value
    assert value is not None
    assert value.project.models["model.shop.m"].config.retries == 1


def test_ids_inputs_and_locations(project_factory: Factory) -> None:
    p = resolve(
        project_factory,
        {
            "models/m.yml": (
                "sources:\n  - {name: raw, format: memory, location: r}\n"
                "models:\n  - name: a\n    sql: select 1\n    retries: 3\n"
                "    inputs: {r: {source: raw}}\n"
            ),
        },
    )
    assert list(p.sources) == ["source.shop.raw"]
    a = p.models["model.shop.a"]
    assert a.inputs["r"].unique_id == "source.shop.raw"
    assert a.inputs["r"].name == "raw"
    assert p.locate_model(a, "retries").line == 6
    assert p.locate_model(a, "timeout").line == 4  # not set: the name mark


def test_merge_settings_tags_ignore_null() -> None:
    cfg = merge_settings([{"tags": ["a"]}, {"tags": None}, {"tags": ["b", "a"]}])
    assert cfg.tags == ["a", "b"]


def test_project_dir_as_model_root(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": PROJECT.replace("name: shop", "name: shop\nmodel_paths: ['.']"),
            "_defaults.yml": "models: {retries: 5, tags: [top]}\n",
            "sub/_defaults.yml": "models: {timeout: 30}\n",
            "m.yml": "models:\n  - {name: m, sql: select 1}\n",
            "sub/n.yml": "models:\n  - {name: n, sql: select 1}\n",
        }
    )
    value = Project(ProjectOptions(project_dir=root, env={})).resolve().value
    assert value is not None
    m = value.project.models["model.shop.m"].config
    n = value.project.models["model.shop.n"].config
    assert (m.retries, m.timeout, m.tags) == (5, None, ["all", "shared", "top"])
    assert (n.retries, n.timeout) == (5, 30)
