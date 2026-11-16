"""The `models:` tree, properties files, source groups and setting origins."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.config.resolve import ResolvedProject, resolve_project
from pipeloom.diagnostics import Report
from pipeloom.validate.references import check_references

Factory = Callable[[Mapping[str, Any]], Path]
BASE = "config_version: 1\nname: shop\ntargets:\n  dev:\n    engine: {kind: memory}\n"

# Highest precedence first: (name, origin layer, file that sets it).
LAYERS = [
    ("model", "model", "models/a/b/m.yml"),
    ("properties", "properties", "models/a/props.yml"),
    ("folder_b", "folder", "pipeloom.yml"),
    ("defaults_b", "defaults_file", "models/a/b/_defaults.yml"),
    ("folder_a", "folder", "pipeloom.yml"),
    ("defaults_root", "defaults_file", "models/_defaults.yml"),
    ("root", "project", "pipeloom.yml"),
    ("project_defaults", "project_defaults", "pipeloom.yml"),
]
TREE = {"folder_b", "folder_a", "root"}


def _settings(layer: str, indent: int, on: set[str]) -> str:
    if layer not in on:
        return ""
    pad, plus = " " * indent, "+" if layer in TREE else ""
    return (
        f"{pad}{plus}write: {{options: {{winner: {layer}, {layer}: 1}}}}\n"
        f"{pad}{plus}partition_by: [{layer}]\n"
        f"{pad}{plus}tags: [{layer}]\n"
    )


def layered_files(on: set[str]) -> dict[str, str]:
    tree = _settings("root", 2, on) + "  a:\n" + _settings("folder_a", 4, on)
    tree += ("    b:\n" + _settings("folder_b", 6, on)) if "folder_b" in on else "    b: {}\n"
    return {
        "pipeloom.yml": BASE
        + "defaults:\n  models:\n"
        + (_settings("project_defaults", 4, on) or "    retries: 0\n")
        + "models:\n"
        + tree,
        "models/_defaults.yml": "models:\n"
        + (_settings("defaults_root", 2, on) or "  retries: 0\n"),
        "models/a/b/_defaults.yml": "models:\n"
        + (_settings("defaults_b", 2, on) or "  retries: 0\n"),
        "models/a/props.yml": "models:\n  - name: m\n    config:\n"
        + (_settings("properties", 6, on) or "      retries: 0\n"),
        "models/a/b/m.yml": "models:\n  - name: m\n    sql: select 1\n" + _settings("model", 4, on),
    }


def resolve(factory: Factory, files: Mapping[str, str], **kw: Any) -> ResolvedProject:
    root = factory(files)
    outcome = Project(ProjectOptions(project_dir=root, env={}, **kw)).resolve()
    assert outcome.value is not None, outcome.diagnostics
    return outcome.value.project


def line_of(files: Mapping[str, str], file: str, needle: str) -> int:
    return next(i for i, ln in enumerate(files[file].splitlines(), 1) if needle in ln)


@pytest.mark.parametrize("top", range(len(LAYERS)), ids=[n for n, _, _ in LAYERS])
def test_each_layer_wins_over_the_ones_below(project_factory: Factory, top: int) -> None:
    on = {name for name, _, _ in LAYERS[top:]}
    files = layered_files(on)
    p = resolve(project_factory, files)
    m = p.models["model.shop.m"]
    winner, layer, file = LAYERS[top]
    below = [name for name, _, _ in reversed(LAYERS[top:])]
    assert m.config.write.options == {"winner": winner, **dict.fromkeys(below, 1)}  # deep merge
    assert m.config.partition_by == [winner]  # lists replace
    assert m.config.tags == below  # ordered union, lowest layer first
    origin = m.origins["partition_by"]
    assert (origin.layer, origin.location.file) == (layer, file)
    assert origin.location.line == line_of(files, file, f"partition_by: [{winner}]")
    assert p.locate_model(m, "partition_by") == origin.location


def test_explicit_null_in_the_tree_removes_a_lower_setting(project_factory: Factory) -> None:
    files = {
        "pipeloom.yml": BASE + "models:\n  +timeout: 60\n  +retries: 2\n  a:\n    +timeout: null\n",
        "models/a/m.yml": "models:\n  - name: m\n    sql: select 1\n",
        "models/n.yml": "models:\n  - name: n\n    sql: select 1\n",
    }
    p = resolve(project_factory, files)
    m, n = p.models["model.shop.m"], p.models["model.shop.n"]
    assert (m.config.timeout, m.config.retries) == (None, 2)
    assert (n.config.timeout, n.config.retries) == (60, 2)
    assert m.origins["timeout"].layer == "folder"
    assert n.origins["timeout"].layer == "project"
    assert m.origins["retries"].location.json_pointer == "/models/+retries"


def test_defaults_files_interleave_with_the_tree(project_factory: Factory) -> None:
    """`_defaults.yml` and `defaults.models` map onto the folder and project layers."""
    files = {
        "pipeloom.yml": BASE
        + "defaults:\n  models:\n    retries: 1\n    timeout: 5\n"
        + "models:\n  +retries: 3\n  a:\n    +retries: 4\n    +timeout: 6\n    b:\n"
        + "      +timeout: 8\n",
        "models/a/b/_defaults.yml": "models:\n  retries: 5\n  timeout: 7\n",
        "models/a/b/m.yml": "models:\n  - name: m\n    sql: select 1\n",
        "models/n.yml": "models:\n  - name: n\n    sql: select 1\n",
    }
    p = resolve(project_factory, files)
    m, n = p.models["model.shop.m"], p.models["model.shop.n"]
    # A nearer `_defaults.yml` beats a farther tree folder and the root `+key`.
    assert (m.config.retries, m.origins["retries"].layer) == (5, "defaults_file")
    # The tree beats `_defaults.yml` at the same folder.
    assert (m.config.timeout, m.origins["timeout"].layer) == (8, "folder")
    # The root `+key` beats `defaults.models`.
    assert (n.config.retries, n.origins["retries"].layer) == (3, "project")
    assert (n.config.timeout, n.origins["timeout"].layer) == (5, "project_defaults")


def test_folder_defaults_beat_the_root_tree_key(project_factory: Factory) -> None:
    files = {
        "pipeloom.yml": BASE + "models:\n  +materialized: table\n",
        "models/staging/_defaults.yml": "models:\n  materialized: view\n",
        "models/staging/s.yml": "models:\n  - name: s\n    sql: select 1\n",
        "models/m.yml": "models:\n  - name: m\n    sql: select 1\n",
    }
    p = resolve(project_factory, files)
    assert p.models["model.shop.s"].config.materialized == "view"
    assert p.models["model.shop.m"].config.materialized == "table"


def test_properties_description_and_columns(project_factory: Factory) -> None:
    files = {
        "pipeloom.yml": BASE,
        "models/m.yml": (
            "models:\n  - name: m\n    sql: select 1 as id\n"
            "  - name: n\n    sql: select 1 as id\n    description: own\n"
            "    columns:\n      - name: id\n        dtype: int64\n"
        ),
        "models/_properties.yml": (
            "models:\n"
            "  - name: m\n    description: From properties.\n"
            "    columns:\n      - name: id\n        dtype: int32\n"
            "  - name: n\n    description: loses\n"
            "    columns:\n      - name: other\n        dtype: string\n"
        ),
    }
    p = resolve(project_factory, files)
    m, n = p.models["model.shop.m"], p.models["model.shop.n"]
    assert m.config.description == "From properties."
    assert "description" not in m.config.model_dump()  # out of checksums and the manifest
    assert [(c.name, c.dtype) for c in m.entry.columns or ()] == [("id", "int32")]
    assert m.origins["columns"].layer == "properties"
    assert m.origins["columns"].location.file == "models/_properties.yml"
    assert "columns" not in m.own_fields
    assert n.config.description == "own"  # the model entry wins
    assert [c.name for c in n.entry.columns or ()] == ["id"]  # entry columns replace whole
    assert n.origins["columns"].layer == "model"


def test_properties_data_tests_and_unit_tests(project_factory: Factory) -> None:
    """ADR-0045: properties data_tests apply unless the model sets its own (list replace);
    unit_tests are read from properties and keep their properties location."""
    files = {
        "pipeloom.yml": BASE,
        "models/m.yml": (
            "models:\n  - name: m\n    sql: select 1 as id\n"
            "  - name: n\n    sql: select 1 as id\n"
            "    data_tests:\n      - name: own\n        check:\n          not_null: [id]\n"
        ),
        "models/_properties.yml": (
            "models:\n"
            "  - name: m\n"
            "    data_tests:\n      - name: t\n        check:\n          unique: [id]\n"
            "    unit_tests:\n      - name: u\n        expect:\n          rows:\n"
            "            - id: 1\n"
            "  - name: n\n"
            "    data_tests:\n      - name: loses\n        check:\n          unique: [id]\n"
        ),
    }
    p = resolve(project_factory, files)
    m, n = p.models["model.shop.m"], p.models["model.shop.n"]
    assert [t.name for t in m.entry.data_tests or ()] == ["t"]
    assert m.origins["data_tests"].layer == "properties"
    loc = p.locate_data_test(m, 0)
    assert (loc.file, loc.json_pointer) == ("models/_properties.yml", "/models/0/data_tests/0")
    assert [t.name for t in n.entry.data_tests or ()] == ["own"]
    assert n.origins["data_tests"].layer == "model"
    assert p.locate_data_test(n, 0).file == "models/m.yml"
    (case,) = p.unit_tests
    assert (case.model, case.name, case.file, case.pointer) == (
        "m",
        "u",
        "models/_properties.yml",
        "/models/0/unit_tests/0",
    )


def test_decorator_data_tests_win_over_properties(project_factory: Factory) -> None:
    """ADR-0050: the decorator's data_tests replace the properties list and are located on
    the decorator; a model without its own keeps the properties list and location."""
    files = {
        "pipeloom.yml": BASE + "python_paths: [models]\n",
        "models/py_models.py": (
            "from pipeloom import DataTest, model\n"
            "\n"
            "\n"
            "@model(\n"
            '    data_tests=[DataTest(name="own", check={"not_null": ["id"]})],\n'
            ")\n"
            "def m():\n"
            '    return [{"id": 1}]\n'
            "\n"
            "\n"
            "@model\n"
            "def n():\n"
            '    return [{"id": 1}]\n'
        ),
        "models/_properties.yml": (
            "models:\n"
            "  - name: m\n"
            "    data_tests:\n      - name: loses\n        check:\n          unique: [id]\n"
            "    unit_tests:\n      - name: u\n        expect:\n          rows:\n"
            "            - id: 1\n"
            "  - name: n\n"
            "    data_tests:\n      - name: t\n        check:\n          unique: [id]\n"
        ),
    }
    p = resolve(project_factory, files)
    m, n = p.models["model.shop.m"], p.models["model.shop.n"]
    assert [t.name for t in m.entry.data_tests or ()] == ["own"]
    origin = m.origins["data_tests"]
    assert origin.layer == "decorator"
    assert (origin.location.file, origin.location.line) == ("models/py_models.py", 5)
    loc = p.locate_data_test(m, 0, "/check")
    assert (loc.file, loc.line, loc.json_pointer) == (
        "models/py_models.py",
        5,
        "/@model/data_tests/0/check",
    )
    assert [t.name for t in n.entry.data_tests or ()] == ["t"]
    assert n.origins["data_tests"].layer == "properties"
    loc = p.locate_data_test(n, 0)
    assert (loc.file, loc.json_pointer) == ("models/_properties.yml", "/models/1/data_tests/0")
    assert [(c.model, c.name, c.file) for c in p.unit_tests] == [
        ("m", "u", "models/_properties.yml")
    ]


def test_properties_cannot_set_inputs(project_factory: Factory) -> None:
    root = project_factory(
        {
            "pipeloom.yml": BASE,
            "models/p.yml": "models:\n  - name: m\n    inputs:\n      a:\n        ref: x\n",
        }
    )
    outcome = Project(ProjectOptions(project_dir=root, env={})).load()
    assert [d.code for d in outcome.diagnostics] == ["PLS101"]


def test_unknown_tree_key_is_pls101_with_a_hint(project_factory: Factory) -> None:
    root = project_factory({"pipeloom.yml": BASE + "models:\n  +materalized: view\n"})
    outcome = Project(ProjectOptions(project_dir=root, env={})).load()
    (d,) = outcome.diagnostics
    assert (d.code, d.hint) == ("PLS101", "did you mean '+materialized'?")
    assert d.location is not None and d.location.line == 7


PLR209_FILES = {
    "pipeloom.yml": BASE,
    "models/m.yml": "models:\n  - name: m\n    sql: select 1\n",
    "models/a.yml": "models:\n  - name: m\n  - name: x\n",
    "models/b.yml": "models:\n  - name: m\n",
}


def test_plr209_and_its_suppression(project_factory: Factory) -> None:
    root = project_factory(PLR209_FILES)
    outcome = Project(ProjectOptions(project_dir=root, env={})).resolve()
    found = [(d.code, d.location.file if d.location else None) for d in outcome.diagnostics]
    assert found == [("PLR209", "models/a.yml"), ("PLR209", "models/b.yml")]
    bound = Project(ProjectOptions(project_dir=root, env={})).bind().value
    assert bound is not None
    report = Report()
    check_references(resolve_project(bound), report, models_incomplete=True)
    assert report.sorted() == ()


SOURCES = (
    BASE.replace("targets:\n  dev:", "default_target: dev\ntargets:\n  dev:")
    + "  prod:\n    engine: {kind: memory}\n"
    "    sources:\n      tpch:\n        format: table\n        location: null\n"
    "        schema: samples.tpch\n        tables:\n"
    "          customer:\n            location: null\n          orders:\n            location: null\n"
    "sources:\n  tpch:\n    format: parquet\n    location: data/tpch/\n    options: {a: 1}\n"
    "    tags: [raw]\n    tables:\n      customer:\n        location: customer.parquet\n"
    "        options: {b: 2}\n      orders:\n        location: orders.parquet\n"
    "      nation:\n        tags: [small]\n"
)
SOURCE_MODEL = (
    "models:\n  - name: m\n    sql: select 1\n    inputs:\n      o:\n        source: tpch.orders\n"
)


@pytest.mark.parametrize("target", ["dev", "prod"])
def test_source_groups_and_target_overrides(project_factory: Factory, target: str) -> None:
    p = resolve(
        project_factory,
        {"pipeloom.yml": SOURCES, "models/m.yml": SOURCE_MODEL},
        target=target,
    )
    assert list(p.sources) == [
        "source.shop.tpch.customer",
        "source.shop.tpch.orders",
        "source.shop.tpch.nation",
    ]
    customer = p.sources["source.shop.tpch.customer"].config
    nation = p.sources["source.shop.tpch.nation"].config
    assert p.models["model.shop.m"].inputs["o"].unique_id == "source.shop.tpch.orders"
    assert p.models["model.shop.m"].inputs["o"].name == "tpch.orders"
    assert nation.tags == ["raw", "small"]
    assert customer.options == {"a": 1, "b": 2}
    if target == "dev":
        assert (customer.format, customer.location) == ("parquet", "data/tpch/customer.parquet")
        assert customer.table is None
        assert (nation.location, nation.table) == (None, "nation")  # the key, no schema
    else:
        assert (customer.format, customer.location) == ("table", None)
        assert customer.table == "samples.tpch.customer"
        assert (nation.location, nation.table) == (None, "samples.tpch.nation")
    loc = p.locate("pipeloom.yml", p.sources["source.shop.tpch.nation"].pointer)
    assert loc.line == line_of({"p": SOURCES}, "p", "tags: [small]")


@pytest.mark.parametrize("location", ["D:/data/orders.parquet", "C:\\\\data\\\\x.parquet"])
def test_absolute_table_location_ignores_the_group_location(
    project_factory: Factory, location: str
) -> None:
    text = SOURCES.replace("location: orders.parquet", f'location: "{location}"')
    p = resolve(project_factory, {"pipeloom.yml": text, "models/m.yml": SOURCE_MODEL})
    assert p.sources["source.shop.tpch.orders"].config.location == location.replace("\\\\", "\\")


def test_source_table_with_both_location_and_table_is_pls108(project_factory: Factory) -> None:
    text = SOURCES.replace("tags: [small]", "location: n.parquet\n        table: nation")
    root = project_factory({"pipeloom.yml": text})
    outcome = Project(ProjectOptions(project_dir=root, env={})).load()
    (d,) = outcome.diagnostics
    assert d.code == "PLS108" and d.location is not None
    assert d.location.json_pointer == "/sources/tpch/tables/nation"


def test_target_override_leaving_both_is_pls108(project_factory: Factory) -> None:
    text = SOURCES.replace(
        "          orders:\n            location: null\n",
        "          orders:\n            table: o\n",
    )
    root = project_factory({"pipeloom.yml": text, "models/m.yml": SOURCE_MODEL})
    outcome = Project(ProjectOptions(project_dir=root, env={}, target="prod")).bind()
    assert outcome.value is None
    found = [(d.code, d.location.json_pointer if d.location else None) for d in outcome.diagnostics]
    assert found == [("PLS108", "/targets/prod/sources/tpch/tables/orders")]


def test_legacy_flat_sources_keep_their_ids(project_factory: Factory) -> None:
    p = resolve(
        project_factory,
        {
            "pipeloom.yml": BASE,
            "models/m.yml": "sources:\n  - {name: raw, format: memory, location: r}\n",
        },
    )
    assert list(p.sources) == ["source.shop.raw"]
