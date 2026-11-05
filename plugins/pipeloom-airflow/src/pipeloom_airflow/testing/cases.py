"""Scenarios for the `python:` case files of `tests/cases`: each takes the project directory the
case file wrote and returns the diagnostics of the code it is named after. They run without
Airflow, so the case files run on every CI cell."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import Diagnostic, Report
from pipeloom.graph.selectors import Selection
from pipeloom.runtime.finalize_run import FinalizeOptions, read_unit_states
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
from pipeloom_airflow.states import unit_states, unit_states_document
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
            kubernetes=KubernetesConfig(
                image="img", pod_kwargs={"cmds": ["x"], "deferrable": True}
            ),
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
    """Finalize a run in which no unit wrote an attempt file, with the states Airflow reports.
    The core turns them into failed units, and PLA004 comes out of its own outcome."""
    states = unit_states({"ta": "success", "tb": "failed"}, {"ta": ("a",), "tb": ("b",)})
    document = unit_states_document(states, compact=False)
    path = project_dir / "unit_states.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    project = Project(ProjectOptions(project_dir=project_dir, target="dev", env={}))
    outcome = project.finalize(
        FinalizeOptions(run_id="case", selection=Selection(), unit_states=read_unit_states(path))
    )
    return [d for d in outcome.diagnostics if d.code == "PLA004"]


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
