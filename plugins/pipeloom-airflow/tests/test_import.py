"""Importing the plugin, and rendering with it, never imports Airflow or the Databricks SDK."""

from __future__ import annotations

import subprocess
import sys

IMPORT = """
import sys
import pipeloom_airflow
from pipeloom_airflow import (config, errors, load, naming, render, commands, states, version)
from pipeloom_airflow.testing import cases
assert "airflow" not in sys.modules, "airflow was imported"
for name in ("kubernetes", "databricks", "databricks.sdk", "pyspark"):
    assert name not in sys.modules, name + " was imported"
print("ok")
"""

LAZY = """
import sys
import pipeloom_airflow
assert not [m for m in sys.modules if m.startswith("pipeloom_airflow.")], "eager import"
assert pipeloom_airflow.ProjectConfig.__module__ == "pipeloom_airflow.config"
try:
    pipeloom_airflow.nope
except AttributeError:
    print("ok")
"""

DATABRICKS = """
import sys, tempfile, pathlib
from pipeloom.diagnostics import Report
from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, RenderConfig
from pipeloom_airflow.load import LoadedPlan
from pipeloom_airflow.render import render_plan
from pipeloom.api import Project, ProjectOptions

root = pathlib.Path(tempfile.mkdtemp())
(root / "models").mkdir()
(root / "pipeloom.yml").write_text(
    "config_version: 1\\nname: p\\nengine: {kind: memory}\\ncatalog: {kind: memory}\\n"
    "deploy: {kind: databricks}\\ntargets:\\n  ci: {}\\n", encoding="utf-8")
(root / "models" / "m.yml").write_text("models:\\n  - {name: a, sql: select 1 as x}\\n", encoding="utf-8")
plan = Project(ProjectOptions(project_dir=root, target="ci", env={})).compile().value
loaded = LoadedPlan(plan, plan.metadata.plan_hash, None, "project")
tasks = render_plan(loaded, "ci", RenderConfig(), ExecutionConfig(mode=ExecutionMode.DATABRICKS), Report())
assert [t.task_id for t in tasks.tasks] == ["a"], tasks
for name in ("airflow", "databricks.sdk", "pyspark", "pipeloom_spark"):
    assert name not in sys.modules, name + " was imported"
print("ok")
"""


def run(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, check=False
    )


def test_importing_the_plugin_leaves_airflow_out_of_sys_modules() -> None:
    proc = run(IMPORT)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"


def test_the_package_is_lazy() -> None:
    proc = run(LAZY)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"


def test_databricks_mode_renders_without_airflow_or_the_sdk() -> None:
    proc = run(DATABRICKS)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"
