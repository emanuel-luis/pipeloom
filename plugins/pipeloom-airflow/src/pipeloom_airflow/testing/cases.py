"""Scenarios for the `python:` case files of `tests/cases`: each takes the project directory the
case file wrote and returns the diagnostics of the code it is named after. They run without
Airflow, so the case files run on every CI cell."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom_airflow.config import (
    ExecutionConfig,
    ExecutionMode,
    KubernetesConfig,
    LoadMode,
    ProjectConfig,
    RenderConfig,
    validate_configs,
)
from pipeloom_airflow.load import LoadedPlan, load_plan
from pipeloom_airflow.render import render_plan
from pipeloom_airflow.states import unit_states
from pipeloom_airflow.version import check_airflow_version


def _loaded(project_dir: Path) -> LoadedPlan:
    outcome = Project(ProjectOptions(project_dir=project_dir, target="dev", env={})).compile()
    assert outcome.value is not None, [d.message for d in outcome.diagnostics]
    plan = outcome.value
    return LoadedPlan(plan, plan.metadata.plan_hash, None, "project")


def pla001(project_dir: Path) -> Sequence[Diagnostic]:
    report = Report()
    validate_configs(
        ProjectConfig("/p", manifest_path="/p/m.json"),
        RenderConfig(),
        ExecutionConfig(mode=ExecutionMode.DOCKER),
        {},
        report,
    )
    return report.sorted()


def pla002(project_dir: Path) -> Sequence[Diagnostic]:
    report = Report()
    validate_configs(
        ProjectConfig("relative/project"),
        RenderConfig(asset_uri="pipeloom://{project}/{schema}"),
        ExecutionConfig(
            mode=ExecutionMode.KUBERNETES,
            kubernetes=KubernetesConfig(image="img", pod_kwargs={"cmds": ["x"]}),
        ),
        {"retries": 3},
        report,
    )
    return report.sorted()


def pla003(project_dir: Path) -> Sequence[Diagnostic]:
    report = Report()
    render_plan(
        _loaded(project_dir),
        "dev",
        RenderConfig(select=("tag:nothing",)),
        ExecutionConfig(),
        report,
    )
    return [d for d in report.sorted() if d.code == "PLA003"]


def pla004(project_dir: Path) -> Sequence[Diagnostic]:
    report = Report()
    states = unit_states(
        {"a": "success", "b": "failed"},
        {"a": ("a",), "b": ("b",)},
    )
    for unit, state in sorted(states.items()):
        report.add("PLA004", f"unit '{unit}': {state.message}")
    return report.sorted()


def pla005(project_dir: Path) -> Sequence[Diagnostic]:
    report = Report()
    check_airflow_version("3.1.0", report)
    check_airflow_version("4.0.0", report)
    return report.sorted()


def pla006(project_dir: Path) -> Sequence[Diagnostic]:
    """The cache directory is a file, so no entry can be written; the plan still loads."""
    blocked = project_dir / "blocked"
    blocked.write_text("a file", encoding="utf-8")
    report = Report()
    loaded = load_plan(
        ProjectConfig(project_dir.as_posix(), cache_dir=blocked.as_posix()),
        RenderConfig(load_method=LoadMode.PROJECT),
        "dev",
        report,
        env={},
    )
    assert loaded is not None
    return [d for d in report.sorted() if d.code == "PLA006"]


def pla007(project_dir: Path) -> Sequence[Diagnostic]:
    report = Report()
    render_plan(_loaded(project_dir), "dev", RenderConfig(), ExecutionConfig(), report)
    return report.sorted()
