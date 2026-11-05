"""Guard for the Airflow-importing tests. Loaded before any test module here imports Airflow.

They run only with PIPELOOM_TEST_AIRFLOW=1 on Linux or macOS with Airflow installed (the
`airflow` group, `scripts/wsl-airflow-tests.sh`); anywhere else nothing here is collected. When
they run, Airflow must point at a throwaway home under the temp directory and a sqlite database
in it, with no connection or secrets backend from the environment: a running Airflow or its
database is never reachable. Anything else stops the session."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from importlib.util import find_spec
from pathlib import Path
from typing import Any

import pytest

HERE = Path(__file__).resolve().parent
PROJECT = HERE / "project"
DAGS = HERE / "dags"


def _reason() -> str | None:
    if os.environ.get("PIPELOOM_TEST_AIRFLOW") != "1":
        return "set PIPELOOM_TEST_AIRFLOW=1 to run the Airflow tests"
    if sys.platform == "win32":
        return "Airflow does not run on Windows"
    if find_spec("airflow") is None:
        return "Airflow is not installed (uv sync --group airflow)"
    return None


def _under(path: str, root: str) -> bool:
    try:
        return Path(path).resolve().is_relative_to(Path(root).resolve())
    except (OSError, ValueError):
        return False


def _isolation_problem() -> str | None:
    home = os.environ.get("AIRFLOW_HOME")
    roots = [r for r in (tempfile.gettempdir(), os.environ.get("RUNNER_TEMP")) if r]
    if not home or not any(_under(home, r) for r in roots):
        return f"AIRFLOW_HOME must be a directory under {roots}, got {home!r}"
    url = os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "")
    prefix = "sqlite:///"
    if not url.startswith(prefix) or not _under(url[len(prefix) :], home):
        return "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN must be a sqlite:/// URL under AIRFLOW_HOME"
    leaked = sorted(k for k in os.environ if k.startswith(("AIRFLOW_CONN_", "AIRFLOW__SECRETS__")))
    if leaked:
        return f"unset {', '.join(leaked)} before running the Airflow tests"
    return None


REASON = _reason()
if REASON is not None:
    collect_ignore_glob = ["test_*.py"]
else:
    _problem = _isolation_problem()
    if _problem is not None:
        pytest.exit(f"Airflow test isolation: {_problem}", returncode=3)


def pytest_report_header() -> str:
    return f"airflow tests: {REASON or 'enabled'}"


@pytest.fixture
def af_project(tmp_path: Path) -> tuple[Path, Path, str]:
    """A copy of the test project with a stable manifest: (project dir, manifest, plan hash)."""
    from pipeloom.api import Project, ProjectOptions
    from pipeloom.plan.manifest import write_manifest

    project = tmp_path / "afproj"
    shutil.copytree(PROJECT, project, ignore=shutil.ignore_patterns("__pycache__"))
    outcome = Project(ProjectOptions(project_dir=project, target="dev", env={})).compile()
    assert outcome.value is not None, [d.message for d in outcome.diagnostics]
    manifest = project / "manifest.json"
    write_manifest(outcome.value, manifest, stable=True)
    return project, manifest, outcome.value.metadata.plan_hash


@pytest.fixture
def af_plan(af_project: tuple[Path, Path, str]) -> tuple[Any, Any]:
    """(TaskPlan, CoreCommand) of the test project in LOCAL mode, loaded from its manifest."""
    from pipeloom_airflow.config import ExecutionConfig, ProjectConfig, RenderConfig
    from pipeloom_airflow.dag import _plan

    project, manifest, _ = af_project
    return _plan(
        ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
        "dev",
        RenderConfig(),
        ExecutionConfig(),
        {},
    )
