"""`pipeloom demo`: the sample project on DuckDB, the docs site, and the PLU004 stop."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.cli.demo import INSTALL, missing_engine
from tests.plugins.fakes import install_entry_points


def cli(*args: str) -> Any:
    return CliRunner().invoke(main, list(args), env={})


def test_demo_builds_and_writes_the_docs(tmp_path: Path) -> None:
    out = tmp_path / "shop"
    result = cli("demo", str(out))
    assert result.exit_code == 0, result.stderr
    page = out / "target" / "docs" / "index.html"
    assert page.is_file()
    assert f"Docs site: {page.as_posix()}" in result.stdout
    assert "pipeloom docs serve" in result.stdout
    assert (out / "target" / "warehouse.duckdb").is_file()
    assert "unit tests: 1 passed" in result.stderr


def test_demo_default_folder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert cli("demo").exit_code == 0
    assert (tmp_path / "pipeloom_demo" / "target" / "docs" / "index.html").is_file()


def test_demo_refuses_a_non_empty_directory(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("x", encoding="utf-8")
    result = cli("demo", str(tmp_path))
    assert result.exit_code == 2
    assert "is not empty" in result.stderr
    assert not (tmp_path / "pipeloom.yml").exists()


def test_demo_without_the_duckdb_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    install_entry_points(monkeypatch, [])
    out = tmp_path / "shop"
    result = cli("demo", str(out), "--log-format", "json")
    assert result.exit_code == 1
    found = [
        json.loads(line)["data"]
        for line in result.stderr.splitlines()
        if json.loads(line)["data"].get("code") == "PLU004"
    ]
    assert found and found[0]["hint"] == INSTALL
    assert not out.exists()


def test_missing_engine_lists_what_is_installed() -> None:
    assert "found: polars" in missing_engine(["polars"]).message
    assert "found: none" in missing_engine([]).message
