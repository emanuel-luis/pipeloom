"""`pipeloom init`: flags, prompts, directory rules, and generated projects that validate and run."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner

import pipeloom
from pipeloom.cli import main
from pipeloom.scaffold import ENGINES, render

# The package attribute `init` is the command, so reach the module through importlib.
init_module = importlib.import_module("pipeloom.cli.init")


def cli(*args: str, input: str | None = None) -> Any:
    return CliRunner().invoke(main, list(args), env={}, input=input)


@pytest.fixture
def tty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(init_module, "_interactive", lambda: True)


def project_yml(root: Path) -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load((root / "pipeloom.yml").read_text(encoding="utf-8"))
    return data


def test_flags(tmp_path: Path) -> None:
    out = tmp_path / "proj"
    result = cli("init", str(out), "--name", "shop", "--engine", "pandas", "--no-input")
    assert result.exit_code == 0, result.output
    doc = project_yml(out)
    assert doc["name"] == "shop"
    assert doc["targets"]["dev"]["engine"]["kind"] == "pandas"
    assert (out / "src" / "shop_models" / "silver.py").is_file()
    assert (out / ".gitignore").is_file()
    assert f"in {out.as_posix()}" in result.stdout
    assert "pipeloom validate" in result.stdout and "pipeloom run" in result.stdout


def test_no_input_defaults(tmp_path: Path) -> None:
    out = tmp_path / "my-proj"
    result = cli("init", str(out), "--no-input")
    assert result.exit_code == 0, result.output
    doc = project_yml(out)
    assert doc["name"] == "my_proj"
    assert doc["targets"]["dev"]["engine"]["kind"] == "polars"


def test_default_directory_is_the_name(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = cli("init", "--name", "shop", "--no-input")
    assert result.exit_code == 0, result.output
    assert (tmp_path / "shop" / "pipeloom.yml").is_file()
    assert "in shop\n" in result.stdout


def test_not_a_terminal_never_prompts(tmp_path: Path) -> None:
    result = cli("init", str(tmp_path / "p"), input="ignored\n")
    assert result.exit_code == 0
    assert "Project name" not in result.output


def test_prompts(tmp_path: Path, tty: None) -> None:
    out = tmp_path / "proj"
    result = cli("init", str(out), input="bad-name\nshop\ndatabricks\nlake\n")
    assert result.exit_code == 0, result.output
    assert "is not a valid name" in result.output
    dev = project_yml(out)["targets"]["dev"]
    assert project_yml(out)["name"] == "shop"
    assert dev["catalog"] == {"kind": "unity", "catalog": "lake"}
    assert dev["state"]["table"] == "lake.pipeloom_ops.pipeloom_state_shop"
    assert dev["artifacts"]["root"] == "/Volumes/lake/pipeloom_ops/artifacts/shop"
    deps = dev["deploy"]["compute"]["default"]["dependencies"]
    assert f"pipeloom=={pipeloom.__version__}" in deps
    assert "pipeloom databricks deploy" in result.stdout


def test_prompt_defaults(tmp_path: Path, tty: None) -> None:
    out = tmp_path / "proj"
    result = cli("init", str(out), input="\n\n")
    assert result.exit_code == 0, result.output
    assert project_yml(out)["name"] == "proj"
    assert project_yml(out)["targets"]["dev"]["engine"]["kind"] == "polars"


def test_flags_skip_their_prompts(tmp_path: Path, tty: None) -> None:
    out = tmp_path / "proj"
    result = cli("init", str(out), "--name", "shop", "--engine", "databricks", "--catalog", "c")
    assert result.exit_code == 0, result.output
    assert "Project name" not in result.output and "Catalog" not in result.output


def test_no_input_flag_beats_terminal(tmp_path: Path, tty: None) -> None:
    out = tmp_path / "proj"
    result = cli("init", str(out), "--engine", "databricks", "--no-input")
    assert result.exit_code == 0, result.output
    assert project_yml(out)["targets"]["dev"]["catalog"]["catalog"] == "main"


def test_catalog_needs_databricks(tmp_path: Path) -> None:
    result = cli("init", str(tmp_path / "p"), "--engine", "polars", "--catalog", "c")
    assert result.exit_code == 2
    assert "--catalog applies only to --engine databricks" in result.stderr
    assert not (tmp_path / "p").exists()


def test_catalog_with_default_engine(tmp_path: Path) -> None:
    assert cli("init", str(tmp_path / "p"), "--catalog", "c", "--no-input").exit_code == 2


def test_bad_name_flag(tmp_path: Path) -> None:
    result = cli("init", str(tmp_path / "p"), "--name", "1x")
    assert result.exit_code == 2 and "is not a valid name" in result.stderr


def test_bad_engine(tmp_path: Path) -> None:
    assert cli("init", str(tmp_path / "p"), "--engine", "duckdb").exit_code == 2


def test_non_empty_directory(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("x", encoding="utf-8")
    result = cli("init", str(tmp_path), "--no-input")
    assert result.exit_code == 2
    assert "is not empty; pass --force" in result.stderr
    assert not (tmp_path / "pipeloom.yml").exists()


def test_force_keeps_existing_files(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("mine\n", encoding="utf-8")
    result = cli("init", str(tmp_path), "--name", "shop", "--no-input", "--force")
    assert result.exit_code == 0, result.output
    assert (tmp_path / "README.md").read_text(encoding="utf-8") == "mine\n"
    assert "Skipped existing file: README.md" in result.stdout
    assert (tmp_path / "pipeloom.yml").is_file()


def test_existing_project_is_refused_even_with_force(tmp_path: Path) -> None:
    (tmp_path / "pipeloom.yml").write_text("name: x\n", encoding="utf-8")
    result = cli("init", str(tmp_path), "--no-input", "--force")
    assert result.exit_code == 2
    assert "already holds a pipeloom project" in result.stderr


def test_interactive_is_stdin_isatty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.stdin.isatty", lambda: True, raising=False)
    assert init_module._interactive() is True


@pytest.mark.parametrize("engine", ENGINES)
def test_yaml_is_block_style(engine: str) -> None:
    values = {
        "name": "shop",
        "engine": engine,
        "catalog": "main",
        "version": "0",
        "run_command": "",
    }
    for path, content in render(engine, values).items():
        if path.endswith(".yml"):
            text = content.decode("utf-8")
            assert "{" not in text.replace("{{", ""), path  # no flow mappings
            assert "{{" not in text, path  # every placeholder filled


@pytest.mark.parametrize("engine", ["polars", "pandas"])
def test_generated_project_validates_and_runs(engine: str, tmp_path: Path) -> None:
    out = tmp_path / "shop"
    assert cli("init", str(out), "--engine", engine, "--no-input").exit_code == 0
    base = ["--project-dir", str(out)]
    result = cli("validate", *base)
    assert result.exit_code == 0, result.stderr
    result = cli("run", *base)
    assert result.exit_code == 0, result.stderr
    gold = out / "target" / "warehouse" / "gold" / "revenue_by_customer"
    assert list(gold.glob("*.parquet"))


@pytest.mark.parametrize("engine", ["spark", "databricks"])
def test_generated_project_validates_and_compiles(engine: str, tmp_path: Path) -> None:
    out = tmp_path / "shop"
    assert cli("init", str(out), "--engine", engine, "--no-input").exit_code == 0
    base = ["--project-dir", str(out)]
    result = cli("validate", *base)
    assert result.exit_code == 0, result.stderr
    result = cli("compile", *base, "--stable")
    assert result.exit_code == 0, result.stderr
    nodes = json.loads((out / "target" / "manifest.json").read_text(encoding="utf-8"))["nodes"]
    relation = nodes["model.shop.revenue_by_customer"]["relation"]
    assert relation["schema"] == "gold" and relation["name"] == "revenue_by_customer"
