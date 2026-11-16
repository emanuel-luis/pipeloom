"""tests/legacy_models.py: legacy YAML models become decorated Python models."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from pipeloom.api import Project, ProjectOptions
from tests import legacy_models as mig

PROJECT = """config_version: 1
name: shop
python_paths: [src]
defaults:
  models:
    materialized: table
targets:
  dev:
    engine:
      kind: memory
"""

FILES = {
    "pipeloom.yml": PROJECT,
    "models/staging/_defaults.yml": "models:\n  tags: [staging]\n",
    "models/staging/sources.yml": (
        "sources:\n  - name: raw_orders\n    format: memory\n    location: raw/orders\n"
    ),
    "models/staging/stg.yml": """models:
  - name: stg_orders
    sql: |
      select *
      from o
    retries: 2
    inputs:
      o:
        source: raw_orders
  - name: enriched
    python: "shop.m:build"
    write:
      strategy: append
    inputs:
      stg_orders:
        ref: stg_orders
      other:
        ref: stg_orders
  - name: stg_orders
    description: Orders.
""",
    "src/shop/__init__.py": "",
    "src/shop/m.py": '"""Mart."""\n\n\ndef build(ctx, stg_orders, other):\n    return stg_orders\n',
}


def test_migrate_converts_every_kind_of_declaration() -> None:
    out = mig.migrate(FILES)
    assert list(out) == [
        "pipeloom.yml",
        "models/staging/stg.yml",
        "models/staging/stg.py",
        "src/shop/__init__.py",
        "src/shop/m.py",
    ]
    project = yaml.safe_load(out["pipeloom.yml"])
    assert "defaults" not in project
    assert project["models"] == {"+materialized": "table", "staging": {"+tags": ["staging"]}}
    assert project["sources"] == {
        "raw": {"tables": {"raw_orders": {"format": "memory", "location": "raw/orders"}}}
    }
    assert project["model_paths"] == ["models", "src"]
    assert project["python_paths"] == ["src", "."]
    assert yaml.safe_load(out["models/staging/stg.yml"]) == {
        "models": [{"name": "stg_orders", "description": "Orders."}]
    }
    assert out["models/staging/stg.py"] == (
        "from pipeloom import model, source\n\n\n"
        "@model(\n"
        '    inputs={"o": source("raw", "raw_orders")},\n'
        "    retries=2,\n"
        ")\n"
        "def stg_orders(ctx, o):\n"
        '    return ctx.sql("""select *\nfrom o\n""")\n'
    )
    assert out["src/shop/m.py"] == (
        '"""Mart."""\n\nfrom pipeloom import model, ref\n\n\n\n'
        "@model(\n"
        '    name="enriched",\n'
        '    inputs={"other": ref("stg_orders")},\n'
        '    write={"strategy": "append"},\n'
        ")\n"
        "def build(ctx, stg_orders, other):\n    return stg_orders\n"
    )


def test_the_migrated_project_validates(tmp_path: Path) -> None:
    files = dict(FILES)
    del files["models/staging/stg.yml"]  # a properties entry needs its model; keep it small
    files["models/staging/stg.yml"] = FILES["models/staging/stg.yml"].split(
        "  - name: stg_orders\n    description"
    )[0]
    for rel, text in mig.migrate(files).items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text, encoding="utf-8")
    outcome = Project(ProjectOptions(project_dir=tmp_path, env={})).validate()
    assert [d.code for d in outcome.diagnostics if d.severity == "error"] == []
    assert outcome.value is not None


def test_names_that_rebind_a_helper_get_name_keyword() -> None:
    code, _ = mig._sql_model({"name": "source", "sql": "select 1"})
    assert (
        code
        == '@model(\n    name="source",\n)\ndef source_(ctx):\n    return ctx.sql("select 1")\n'
    )


def test_a_self_reference_is_listed() -> None:
    code, helpers = mig._sql_model({"name": "a", "sql": "x", "inputs": {"a": {"ref": "a"}}})
    assert 'inputs={"a": ref("a")}' in code and helpers == {"ref"}


@pytest.mark.parametrize(
    ("files", "message"),
    [
        (
            {"pipeloom.yml": PROJECT, "models/m.yml": "models:\n  - name: a\n    python: x:a\n"},
            "module",
        ),
        (
            {
                "pipeloom.yml": PROJECT,
                "models/m.yml": "models:\n  - name: a\n    python: m:f\n",
                "src/m.py": "def f(context):\n    return 1\n",
            },
            "first parameter",
        ),
    ],
)
def test_what_it_cannot_convert_raises(files: dict[str, str], message: str) -> None:
    with pytest.raises(mig.MigrationError, match=message):
        mig.migrate(files)


def test_a_case_file_keeps_its_other_keys(tmp_path: Path) -> None:
    case = tmp_path / "X.yml"
    case.write_text(
        "files:\n  pipeloom.yml: |\n"
        + "".join("    " + ln + "\n" for ln in PROJECT.splitlines())
        + "  models/m.yml: |\n    models:\n      - name: a\n        sql: select 1\nrun: true\n",
        encoding="utf-8",
    )
    assert mig.main([str(case), "--case"]) == 0
    spec = yaml.safe_load(case.read_text(encoding="utf-8"))
    assert spec["run"] is True
    assert list(spec["files"]) == ["pipeloom.yml", "models/m.py"]
    assert mig.main([str(case), "--case"]) == 0  # idempotent


def test_a_project_dir_is_rewritten_in_place(tmp_path: Path) -> None:
    for rel, text in FILES.items():
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / rel).write_text(text, encoding="utf-8")
    assert mig.main([str(tmp_path)]) == 0
    assert not (tmp_path / "models/staging/_defaults.yml").exists()
    assert not (tmp_path / "models/staging/sources.yml").exists()
    assert (tmp_path / "models/staging/stg.py").exists()
    bad = tmp_path / "bad"
    (bad / "models").mkdir(parents=True)
    (bad / "pipeloom.yml").write_text(PROJECT, encoding="utf-8")
    (bad / "models/m.yml").write_text("models:\n  - name: a\n    python: x:a\n", encoding="utf-8")
    assert mig.main([str(bad)]) == 1
