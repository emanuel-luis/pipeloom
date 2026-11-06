"""Editor setup written by `pipeloom init`: schema modelines and .vscode settings."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner
from jsonschema import Draft202012Validator

from pipeloom.cli import main
from pipeloom.config.schema import SCHEMA_BASE_URL, export_schemas
from pipeloom.diagnostics import Report
from pipeloom.plugins import PluginRegistry
from pipeloom.scaffold import ENGINES, render

MODELINE = "# yaml-language-server: $schema="


def installed_schemas() -> dict[str, dict[str, Any]]:
    """The exported schemas with every installed engine plugin, as the docs site publishes them."""
    report = Report()
    schemas = export_schemas(PluginRegistry.discover().load_all(report))
    assert report.sorted() == ()
    return schemas


def expected_schema(rel: str) -> str:
    """The schema file a project YAML file maps to."""
    name = rel.rsplit("/", 1)[-1]
    if rel == "pipeloom.yml":
        return "project.json"
    return "defaults.json" if name == "_defaults.yml" else "models.json"


def glob_match(pattern: str, rel: str) -> bool:
    """VS Code style glob: `**/` spans folders, `*` stays inside one segment."""
    rx = re.escape(pattern).replace(r"\*\*/", "(?:.*/)?").replace(r"\*", "[^/]*")
    return re.fullmatch(rx, rel) is not None


def schemas_for(rel: str, mapping: dict[str, list[str]]) -> set[str]:
    """Schema URLs that apply to `rel`: a positive glob matches and no negative glob does."""
    out: set[str] = set()
    for url, globs in mapping.items():
        hit = any(glob_match(g, rel) for g in globs if not g.startswith("!"))
        miss = any(glob_match(g[1:], rel) for g in globs if g.startswith("!"))
        if hit and not miss:
            out.add(url)
    return out


def check_yaml_file(rel: str, text: str) -> None:
    schema_file = expected_schema(rel)
    assert text.startswith(f"{MODELINE}{SCHEMA_BASE_URL}{schema_file}\n"), rel
    Draft202012Validator(installed_schemas()[schema_file]).validate(yaml.safe_load(text))


@pytest.mark.parametrize("engine", ENGINES)
def test_every_yaml_has_its_modeline_and_validates(engine: str) -> None:
    values = {
        "name": "shop",
        "engine": engine,
        "catalog": "main",
        "version": "0",
        "run_command": "",
    }
    files = render(engine, values)
    yamls = [p for p in files if p.endswith(".yml")]
    assert "pipeloom.yml" in yamls and any(p.endswith("_defaults.yml") for p in yamls)
    for rel in yamls:
        check_yaml_file(rel, files[rel].decode("utf-8"))


@pytest.mark.parametrize("engine", ENGINES)
def test_vscode_settings_map_the_published_schemas(engine: str) -> None:
    values = {
        "name": "shop",
        "engine": engine,
        "catalog": "main",
        "version": "0",
        "run_command": "",
    }
    files = render(engine, values)
    mapping = json.loads(files[".vscode/settings.json"])["yaml.schemas"]
    assert set(mapping) == {SCHEMA_BASE_URL + name for name in installed_schemas()}
    for rel in (p for p in files if p.endswith(".yml")):
        assert schemas_for(rel, mapping) == {SCHEMA_BASE_URL + expected_schema(rel)}, rel
    assert schemas_for("models/x/new.yaml", mapping) == {SCHEMA_BASE_URL + "models.json"}
    assert schemas_for("models/x/_defaults.yaml", mapping) == {SCHEMA_BASE_URL + "defaults.json"}
    assert schemas_for("pipeloom.yaml", mapping) == {SCHEMA_BASE_URL + "project.json"}
    assert schemas_for("other/a.yml", mapping) == set()


def test_vscode_recommends_the_yaml_extension() -> None:
    values = {"name": "shop", "engine": "polars", "catalog": "", "version": "0", "run_command": ""}
    data: Any = json.loads(render("polars", values)[".vscode/extensions.json"])
    assert data["recommendations"] == ["redhat.vscode-yaml"]


def test_init_writes_vscode_files_and_force_keeps_existing_ones(tmp_path: Path) -> None:
    (tmp_path / ".vscode").mkdir()
    (tmp_path / ".vscode" / "settings.json").write_text("{}\n", encoding="utf-8")
    result = CliRunner().invoke(
        main, ["init", str(tmp_path), "--name", "shop", "--no-input", "--force"], env={}
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / ".vscode" / "settings.json").read_text(encoding="utf-8") == "{}\n"
    assert "Skipped existing file: .vscode/settings.json" in result.stdout
    assert (tmp_path / ".vscode" / "extensions.json").is_file()
