from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import format_diagnostic
from tests.conftest import make_project

CASES = Path(__file__).parent.parent / "cases"


@pytest.mark.parametrize("case_file", sorted(CASES.glob("*.yml")), ids=lambda p: p.stem)
def test_case(case_file: Path, tmp_path: Path, golden: Callable[[str, str], None]) -> None:
    code = case_file.stem
    spec: dict[str, Any] = yaml.safe_load(case_file.read_text(encoding="utf-8"))
    project_dir = make_project(tmp_path, spec["files"])
    options = ProjectOptions(project_dir=project_dir, env={})
    outcome = Project(options).load()
    assert code in {d.code for d in outcome.diagnostics}
    text = "".join(format_diagnostic(d) + "\n" for d in outcome.diagnostics)
    as_json = json.dumps([d.to_json() for d in outcome.diagnostics], indent=2, ensure_ascii=True)
    golden(f"diagnostics/{code}.txt", text)
    golden(f"diagnostics/{code}.json", as_json + "\n")
