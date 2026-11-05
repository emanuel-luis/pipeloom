from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom_airflow.load import LoadedPlan

SHOP = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "shop"


def compile_loaded(project_dir: Path, target: str = "ci") -> LoadedPlan:
    outcome = Project(ProjectOptions(project_dir=project_dir, target=target, env={})).compile()
    assert outcome.value is not None, [d.message for d in outcome.diagnostics]
    plan = outcome.value
    return LoadedPlan(plan, plan.metadata.plan_hash, None, "project")


@pytest.fixture(scope="session")
def shop() -> LoadedPlan:
    return compile_loaded(SHOP)


@pytest.fixture
def models_loaded(tmp_path: Path) -> Callable[..., LoadedPlan]:
    """Compile a generated project: path -> text for the model files, plus extra pipeloom.yml
    lines for the top level."""

    def make(files: Mapping[str, str], extra: str = "") -> LoadedPlan:
        for rel, text in files.items():
            target = tmp_path / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        (tmp_path / "pipeloom.yml").write_text(
            "config_version: 1\nname: gen\nengine: {kind: memory}\ncatalog: {kind: memory}\n"
            f"{extra}targets:\n  ci:\n    naming: {{kind: none}}\n",
            encoding="utf-8",
        )
        return compile_loaded(tmp_path)

    return make
