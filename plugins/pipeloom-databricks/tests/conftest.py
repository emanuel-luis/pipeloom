from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import Report
from pipeloom.plan.model import Plan
from pipeloom_databricks.render import Locations, RenderedJob, render_job

GOLDENS = Path(__file__).parent / "goldens"
SHOP = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "shop"
VERSIONS = {"pipeloom": "1.2.3", "pipeloom-databricks": "1.2.3"}
"""Fixed so a release bump leaves the goldens alone."""
LOCS = Locations(
    project_root="/Workspace/pipeloom/shop",
    manifest_path="/Workspace/pipeloom/shop/manifest.json",
    artifacts_root="/Volumes/main/ops/pipeloom",
    state_shared=True,
)


def pytest_addoption(parser: pytest.Parser) -> None:
    # The root tests/conftest.py registers the same flag; this keeps it usable when only the
    # plugin tests are collected.
    try:
        parser.addoption(
            "--update-goldens", action="store_true", default=False, help="Rewrite golden files."
        )
    except ValueError:
        pass


Deploy = Callable[..., Plan]


def compile_dir(project_dir: Path) -> Plan:
    outcome = Project(ProjectOptions(project_dir=project_dir, target="ci", env={})).compile()
    assert outcome.value is not None, [d.message for d in outcome.diagnostics]
    return outcome.value


def with_deploy(plan: Plan, **options: Any) -> Plan:
    """The plan as if its target had this deploy block (the core records it, never builds it)."""
    deploy = {"kind": "databricks", **options}
    return plan.model_copy(update={"metadata": plan.metadata.model_copy(update={"deploy": deploy})})


@pytest.fixture
def locs() -> Locations:
    return LOCS


@pytest.fixture
def versions() -> dict[str, str]:
    return dict(VERSIONS)


Render = Callable[..., tuple[RenderedJob | None, Report]]


@pytest.fixture
def render() -> Render:
    """render(plan, pipeline="main", locs=LOCS) -> (job or None, the report)."""

    def run(
        plan: Plan, pipeline: str = "main", where: Locations = LOCS
    ) -> tuple[RenderedJob | None, Report]:
        report = Report()
        return render_job(plan, pipeline, where, report, versions=VERSIONS), report

    return run


@pytest.fixture(scope="session")
def shop() -> Plan:
    """The shop fixture, compiled for its `ci` target, with no deploy block."""
    return compile_dir(SHOP)


@pytest.fixture
def deploy() -> Deploy:
    return with_deploy


@pytest.fixture
def models_plan(tmp_path: Path) -> Callable[[Mapping[str, str]], Plan]:
    """Compile a project from model files: path under the project -> YAML text.

    A model entry may be written as a flow mapping; every model is a SQL table.
    """

    def make(files: Mapping[str, str]) -> Plan:
        for rel, text in files.items():
            target = tmp_path / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text, encoding="utf-8")
        (tmp_path / "pipeloom.yml").write_text(
            "config_version: 1\nname: gen\nengine: {kind: memory}\ncatalog: {kind: memory}\n"
            "defaults:\n  models: {materialized: table}\ntargets:\n  ci:\n    naming: {kind: none}\n",
            encoding="utf-8",
        )
        return compile_dir(tmp_path)

    return make


@pytest.fixture
def golden(request: pytest.FixtureRequest) -> Callable[[str, bytes], None]:
    """Compare bytes with tests/goldens/<name>; --update-goldens rewrites it (LF)."""
    update = bool(request.config.getoption("--update-goldens", default=False))

    def check(name: str, data: bytes) -> None:
        path = GOLDENS / name
        text = data.decode("utf-8")
        if update:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8", newline="\n") as fh:
                fh.write(text)
            return
        assert path.exists(), f"missing golden {name}; run pytest --update-goldens"
        assert text == path.read_bytes().decode("utf-8").replace("\r\n", "\n")

    return check
