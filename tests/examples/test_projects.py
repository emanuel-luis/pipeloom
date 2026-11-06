"""The example projects under examples/: they validate and compile, and polars matches pandas.

The parity test needs generated data, so it runs only with PIPELOOM_TEST_EXAMPLES=1
(make examples-data, then make examples-test).
"""

from __future__ import annotations

import json
import os
import re
import shutil
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, ClassVar

import polars as pl
import pytest
from click.testing import CliRunner

from pipeloom.cli import main

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if not EXAMPLES.is_dir():  # the sdist ships tests/ without examples/
    pytest.skip("examples/ is not in this source tree", allow_module_level=True)
DATA = EXAMPLES / "data" / "tpch"
PROJECTS = ["tpch_polars", "tpch_pandas", "tpch_spark", "tpch_databricks"]
GOLD = ["revenue_by_region_month", "top_customers"]

needs_data = pytest.mark.skipif(
    os.environ.get("PIPELOOM_TEST_EXAMPLES") != "1",
    reason="set PIPELOOM_TEST_EXAMPLES=1 (after make examples-data) to run the parity test",
)


def cli(*args: str) -> Any:
    return CliRunner().invoke(main, list(args), env={})


@pytest.mark.parametrize("project", PROJECTS)
def test_validate_and_compile(project: str, tmp_path: Path) -> None:
    base = ["--project-dir", str(EXAMPLES / project), "--target", "dev"]
    assert cli("validate", *base).exit_code == 0
    out = tmp_path / "manifest.json"
    result = cli("compile", *base, "--stable", "--out", str(out))
    assert result.exit_code == 0, result.stderr
    nodes = json.loads(out.read_text(encoding="utf-8"))["nodes"]
    assert nodes[f"model.{project}.top_customers"]["relation"] == {
        "catalog": "pipeloom_gold",
        "schema": "tpch",
        "name": "top_customers",
    }
    assert len(nodes) == 9


class _Tags(HTMLParser):
    VOID: ClassVar[set[str]] = {"meta", "input", "br", "hr", "link", "img"}

    def __init__(self) -> None:
        super().__init__()
        self.stack: list[str] = []
        self.bad: list[str] = []
        self.ids: set[str] = set()
        self.node_ids: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = dict(attrs)
        if a.get("id"):
            self.ids.add(a["id"] or "")
        if a.get("data-id"):
            self.node_ids.append(a["data-id"] or "")
        if tag not in self.VOID:
            self.stack.append(tag)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        pass

    def handle_endtag(self, tag: str) -> None:
        if not self.stack or self.stack.pop() != tag:
            self.bad.append(tag)


@pytest.mark.parametrize("project", PROJECTS)
def test_docs_generate_is_one_offline_file(project: str, tmp_path: Path) -> None:
    base = ["--project-dir", str(EXAMPLES / project), "--target", "dev"]
    out = tmp_path / "site"
    result = cli("docs", "generate", *base, "--out", str(out))
    assert result.exit_code == 0, result.stderr
    assert [p.name for p in out.iterdir()] == ["index.html"]
    page = (out / "index.html").read_text(encoding="utf-8")

    assert 'http-equiv="Content-Security-Policy"' in page
    assert "default-src 'none'" in page
    pattern = r"""(?:src=|<link\b[^>]*|url\(|@import\s*)["']?\s*(?:https?:)?//"""
    external = re.compile(pattern, re.IGNORECASE)
    assert not external.search(page)

    tags = _Tags()
    tags.feed(page)
    tags.close()
    assert tags.bad == [] and tags.stack == []

    manifest = tmp_path / "m.json"
    assert cli("compile", *base, "--out", str(manifest)).exit_code == 0
    doc = json.loads(manifest.read_text(encoding="utf-8"))
    for node in doc["nodes"].values():
        assert f"model-{node['name']}" in tags.ids
    for source in doc["sources"].values():
        assert f"source-{source['name']}" in tags.ids
    assert len(tags.node_ids) == len(doc["nodes"]) + len(doc["sources"])
    assert set(tags.node_ids) == set(doc["nodes"]) | set(doc["sources"])


def gold(root: Path, name: str) -> pl.DataFrame:
    table = root / "target" / "warehouse" / "pipeloom_gold" / "tpch" / name
    return pl.read_parquet(sorted(table.glob("*.parquet"))).sort(pl.all())


@needs_data
@pytest.mark.examples
def test_polars_and_pandas_agree(tmp_path: Path) -> None:
    if not (DATA / "orders.parquet").exists():
        pytest.fail(f"no data in {DATA}: run make examples-data")
    roots = {}
    for project in ("tpch_polars", "tpch_pandas"):
        roots[project] = tmp_path / project
        shutil.copytree(
            EXAMPLES / project,
            roots[project],
            ignore=shutil.ignore_patterns("target", ".pipeloom", "__pycache__"),
        )
        result = cli(
            "run",
            "--project-dir",
            str(roots[project]),
            "--target",
            "dev",
            "--vars",
            json.dumps({"tpch_dir": DATA.as_posix()}),
        )
        assert result.exit_code == 0, result.stderr
    for name in GOLD:
        left, right = (gold(roots[p], name) for p in ("tpch_polars", "tpch_pandas"))
        assert left.schema == right.schema, name
        assert left.height == right.height > 0, name
        for column in left.columns:
            a, b = left[column], right[column]
            if a.dtype.is_float():
                # Summation order differs per engine, so a sum can land on either side of a
                # rounding boundary: allow one cent.
                assert ((a - b).abs() <= 0.0101).all(), f"{name}.{column}"
            else:
                assert a.to_list() == b.to_list(), f"{name}.{column}"
