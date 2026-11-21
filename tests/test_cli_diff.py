"""`pipeloom diff` (ADR-0053): seeded relations on polars and DuckDB give exact counts."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli import main
from pipeloom.engine.types import Column, WriteConfig
from pipeloom.plan.manifest import write_manifest
from pipeloom.runtime.context import RuntimeContext
from pipeloom.testing.clock import FakeClock
from tests.conftest import make_project

ENV = {"PIPELOOM_USER": "tester", "PIPELOOM_TARGET": "", "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD": ""}
ENGINES = {
    "polars": "engine: {kind: polars}\ncatalog: {kind: filesystem, root: wh}\n",
    "duckdb": "engine: {kind: duckdb}\ncatalog: {kind: duckdb, path: wh/warehouse.duckdb}\n",
}
MODEL = """from pipeloom import model


@model(
    write={"strategy": "merge", "unique_key": ["id"]},
    columns=[
        {"name": "id", "dtype": "int64"},
        {"name": "name", "dtype": "string"},
        {"name": "amount", "dtype": "int64"},
        {"name": "email", "dtype": "string", "sensitive": True},
    ],
)
def orders(ctx):
    return ctx.sql("select 1 as id")


@model
def keyless(ctx):
    return ctx.sql("select 1 as id")
"""
BASE = (Column("id", "int64"), Column("name", "string"), Column("amount", "int64"))
EMAIL = Column("email", "string")
SOURCE: list[dict[str, Any]] = [
    {"id": 1, "name": "alpha", "amount": 101, "email": "alpha@example.com"},
    {"id": 2, "name": "bravo", "amount": 202, "email": "bravo@example.com"},
    {"id": 3, "name": "charlie", "amount": 303, "email": "charlie@example.com"},
    {"id": 4, "name": "delta", "amount": 404, "email": "delta@example.com"},
    {"id": 6, "name": "foxtrot", "amount": 606, "email": "fox1@example.com"},
    {"id": 6, "name": "foxtrot", "amount": 607, "email": "fox2@example.com"},
    {"id": 7, "name": "golf", "amount": None, "email": "golf@example.com"},
    {"id": None, "name": "hotel", "amount": 808, "email": "hotel@example.com"},
]
TARGET: list[dict[str, Any]] = [
    {"id": 1, "name": "alpha", "amount": 101, "email": "alpha@example.com", "note": "n1"},
    {"id": 2, "name": "bravo", "amount": 2020, "email": "bravo@example.com", "note": "n2"},
    {"id": 3, "name": None, "amount": 303, "email": "charlie@example.com", "note": "n3"},
    {"id": 5, "name": "echo", "amount": 505, "email": "echo@example.com", "note": "n5"},
    {"id": 6, "name": "foxtrot", "amount": 606, "email": "fox1@example.com", "note": "n6"},
    {"id": 7, "name": "golf", "amount": None, "email": "golf@example.com", "note": "n7"},
]
VALUES = ("alpha", "bravo", "2020", "echo", "example.com", "n5", "404")


def _seed(
    root: Path, target: str, rows: Sequence[dict[str, Any]], schema: tuple[Column, ...]
) -> None:
    project = Project(ProjectOptions(project_dir=root, target=target, env=ENV))
    plan, bound = project.compile().value, project.bind().value
    assert plan is not None and bound is not None
    engine = bound.plugins["engines"].create(project.secrets)
    catalog = bound.plugins["catalogs"].create(project.secrets)
    rt = RuntimeContext(
        run_id="seed",
        invocation_id="seed",
        project=plan.metadata.project,
        target=target,
        project_dir=root,
        clock=FakeClock(),
        identity=project.identity,
        vars={},
    )
    with engine.session(rt, catalog=catalog) as session:
        for name in ("orders", "keyless"):
            ref = plan.nodes[f"model.dd.{name}"].relation
            assert ref is not None
            catalog.ensure_namespace(ref.namespace)
            rel = session.from_rows(rows, schema)
            session.materializer("overwrite").materialize(
                rel, ref, WriteConfig("overwrite"), f"{target}-{name}"
            )
    if target == "prod":
        write_manifest(plan, root / "state" / "manifest.json", stable=True)


@pytest.fixture(params=sorted(ENGINES))
def seeded(request: pytest.FixtureRequest, tmp_path: Path) -> Path:
    if request.param == "duckdb":
        pytest.importorskip("pipeloom_duckdb")
    make_project(
        tmp_path,
        {
            "pipeloom.yml": "config_version: 1\nname: dd\nmodel_paths: [models]\n"
            + ENGINES[request.param]
            + "targets:\n"
            "  prod:\n    naming: {kind: none}\n"
            "  dev:\n    naming: {kind: prefix, schema_prefix: dev_}\n",
            "models/orders.py": MODEL,
        },
    )
    _seed(tmp_path, "prod", SOURCE, (*BASE, EMAIL))
    _seed(tmp_path, "dev", TARGET, (*BASE, EMAIL, Column("note", "string")))
    return tmp_path


def diff(root: Path, *args: str) -> Any:
    return CliRunner().invoke(
        main, ["diff", *args, "--project-dir", str(root), "--state", "state"], env=ENV
    )


def test_counts_are_exact(seeded: Path) -> None:
    done = diff(seeded, "orders", "--target", "dev", "--output", "json")
    assert done.exit_code == 1, done.stderr
    doc = json.loads(done.stdout)
    assert doc["schema"] == "pipeloom.table_diff/1"
    assert doc["counts"] == {
        "matched": 2,
        "partial": 2,
        "source_only": 1,
        "target_only": 1,
        "duplicate_keys": {"source": 1, "target": 0},
        "null_keys": {"source": 1, "target": 0},
    }
    assert doc["source"]["rows"] == 8 and doc["target"]["rows"] == 6
    assert doc["schema_diff"]["added"] == [{"name": "note", "dtype": "string"}]
    assert doc["columns"] == [
        {"name": "name", "differs": 1},
        {"name": "amount", "differs": 1},
        {"name": "email", "differs": 0},
    ]
    assert "samples" not in doc


def test_default_markdown_has_no_cell_values(
    seeded: Path, golden: Callable[[str, str], None]
) -> None:
    done = diff(seeded, "orders", "--target", "dev")
    assert done.exit_code == 1, done.stderr
    assert not [v for v in VALUES if v in done.stdout]
    golden("table_diff/default.md", done.stdout)


def test_samples_mask_sensitive_columns(seeded: Path, golden: Callable[[str, str], None]) -> None:
    done = diff(seeded, "orders", "--target", "dev", "--samples", "--output", "json")
    assert done.exit_code == 1, done.stderr
    doc = json.loads(done.stdout)
    golden("table_diff/samples.json", json.dumps(doc, indent=2) + "\n")
    (only,) = doc["samples"]["source_only"]
    assert only == {"id": 4, "name": "delta", "amount": 404, "email": "***"}
    assert [r["changes"] for r in doc["samples"]["partial"]] == [
        {"amount": {"source": 202, "target": 2020}},
        {"name": {"source": "charlie", "target": None}},
    ]
    text = diff(seeded, "orders", "--target", "dev", "--samples")
    assert "delta" in text.stdout and "delta@example.com" not in text.stdout


def test_duplicate_keys_are_reported(seeded: Path) -> None:
    done = diff(seeded, "orders", "--target", "prod", "--key", "name")
    assert done.exit_code == 1  # name repeats (foxtrot twice): reported, not a crash
    assert json.loads(diff(seeded, "orders", "--target", "prod", "--output", "json").stdout)[
        "counts"
    ]["duplicate_keys"] == {"source": 1, "target": 1}
    keyless = diff(seeded, "keyless", "--target", "dev", "--key", "id", "--key", "name")
    assert keyless.exit_code == 1
    same = CliRunner().invoke(
        main,
        ["diff", "orders", "--project-dir", str(seeded), "--state", "state", "--target", "dev"],
        env={**ENV, "PIPELOOM_TARGET": "prod"},
    )
    assert same.exit_code == 1  # --target wins over PIPELOOM_TARGET


def test_no_differences_exit_0(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": "config_version: 1\nname: dd\nmodel_paths: [models]\n"
            + ENGINES["polars"]
            + "targets:\n  prod:\n    naming: {kind: none}\n"
            "  dev:\n    naming: {kind: prefix, schema_prefix: dev_}\n",
            "models/orders.py": MODEL,
        },
    )
    rows = [r for r in SOURCE if r["id"] in (1, 2, 7)]
    _seed(tmp_path, "prod", rows, (*BASE, EMAIL))
    _seed(tmp_path, "dev", rows, (*BASE, EMAIL))
    done = diff(tmp_path, "orders", "--target", "dev", "--log-format", "json")
    assert done.exit_code == 0, done.stderr
    assert "| Matched | 3 |" in done.stdout
    assert all(json.loads(line)["type"] for line in done.stderr.splitlines())


def test_state_is_required(tmp_path: Path) -> None:
    done = CliRunner().invoke(main, ["diff", "orders", "--project-dir", str(tmp_path)], env=ENV)
    assert done.exit_code == 2 and "--state" in done.stderr


def test_vars_and_plugin_options_are_accepted(seeded: Path) -> None:
    done = diff(
        seeded,
        "orders",
        "--target",
        "dev",
        "--vars",
        "{x: 1}",
        "--plugin",
        "pipeloom_polars.spec:SPEC",
    )
    assert done.exit_code == 1, done.stderr


def test_only_key_columns_left_to_compare(seeded: Path) -> None:
    keys = ("--key", "id", "--key", "name", "--key", "amount", "--key", "email")
    done = diff(seeded, "orders", "--target", "dev", *keys, "--samples", "--output", "json")
    assert done.exit_code == 1, done.stderr
    doc = json.loads(done.stdout)
    assert doc["columns"] == [] and doc["counts"]["partial"] == 0
    assert doc["counts"]["matched"] == 2  # ids 1 and 6 with null-free equal keys
    assert doc["schema_diff"]["added"] == [{"name": "note", "dtype": "string"}]


def test_a_retyped_key_is_refused_without_cell_values(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": "config_version: 1\nname: dd\nmodel_paths: [models]\n"
            + ENGINES["polars"]
            + "targets:\n  prod:\n    naming: {kind: none}\n"
            "  dev:\n    naming: {kind: prefix, schema_prefix: dev_}\n",
            "models/orders.py": MODEL,
        },
    )
    _seed(tmp_path, "prod", [{"id": 1, "name": "alpha"}], BASE[:2])
    _seed(
        tmp_path,
        "dev",
        [{"id": "secret@x.com", "name": "alpha"}],
        (Column("id", "string"), BASE[1]),
    )
    done = diff(tmp_path, "orders", "--target", "dev")
    assert done.exit_code == 1 and done.stdout == ""
    assert "PLX131" in done.stderr and "id is int64 in the source and string" in done.stderr
    assert "secret" not in done.stderr
