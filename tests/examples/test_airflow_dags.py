"""The Astro DAG files parse as Python and point at existing example projects.

Airflow does not run on Windows, so the files are only parsed, never imported.
"""

import ast
import re
from pathlib import Path

import pytest

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"
if not EXAMPLES.is_dir():  # the sdist ships tests/ without examples/
    pytest.skip("examples/ is not in this source tree", allow_module_level=True)
DAGS = sorted((EXAMPLES / "airflow" / "dags").glob("*.py"))
ROOT = "/usr/local/airflow/examples/"


def test_dags_exist() -> None:
    assert len(DAGS) == 4


@pytest.mark.parametrize("dag", DAGS, ids=lambda p: p.name)
def test_dag_parses_and_project_exists(dag: Path) -> None:
    source = dag.read_text(encoding="utf-8")
    ast.parse(source)
    match = re.search(re.escape(ROOT) + r"([\w-]+)", source)
    assert match, "no project_dir under the mounted examples folder"
    assert (EXAMPLES / match.group(1) / "pipeloom.yml").is_file()
