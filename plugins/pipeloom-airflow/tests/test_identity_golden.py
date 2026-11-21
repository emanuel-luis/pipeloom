"""Dataset names are frozen at 1.0.0 (ADR-0055): Airflow asset URIs, unit graph assets and
OpenLineage dataset namespaces and names for the shop fixture and the TPC-H examples.

The golden was computed from the 1.0.0 code before the identity function existed. Set
PIPELOOM_UPDATE_GOLDENS=1 to rewrite it, and only for a change that is meant to break 1.x names.
"""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.unit_graph import unit_graph_dict
from pipeloom.plan.manifest import manifest_dict
from pipeloom.plan.model import Plan
from pipeloom_airflow.config import ExecutionConfig, RenderConfig
from pipeloom_airflow.load import LoadedPlan
from pipeloom_airflow.render import render_plan

pytest.importorskip("openlineage.client")
from pipeloom.diagnostics import Report
from pipeloom_openlineage.sink import unit_lineage

ROOT = Path(__file__).resolve().parents[3]
GOLDEN = ROOT / "tests" / "goldens" / "identity" / "dataset-names-1.0.json"
PROJECTS = {
    "shop": (ROOT / "tests" / "fixtures" / "shop", "ci"),
    "tpch_polars": (ROOT / "examples" / "tpch_polars", "dev"),
    "tpch_pandas": (ROOT / "examples" / "tpch_pandas", "dev"),
    "tpch_spark": (ROOT / "examples" / "tpch_spark", "dev"),
}
ALL_PLACEHOLDERS = "a://{project}/{target}/{model}/{relation}/{unique_id}"


def compile_plan(project_dir: Path, target: str) -> Plan:
    outcome = Project(ProjectOptions(project_dir=project_dir, target=target, env={})).compile()
    assert outcome.value is not None, [d.message for d in outcome.diagnostics]
    return outcome.value


def airflow_assets(plan: Plan, target: str, **render: Any) -> dict[str, list[str]]:
    loaded = LoadedPlan(plan, plan.metadata.plan_hash, None, "project")
    tasks = render_plan(
        loaded, target, RenderConfig(emit_assets=True, **render), ExecutionConfig(), Report()
    ).tasks
    return {t.task_id: list(t.assets) for t in tasks}


def openlineage(plan: Plan, project_dir: Path, **options: Any) -> dict[str, Any]:
    manifest = manifest_dict(plan)
    here = project_dir.resolve().as_posix()
    out: dict[str, Any] = {}
    for unit_id in sorted(plan.units):
        event = unit_lineage(manifest, project_dir.resolve(), unit_id, **options)
        out[unit_id] = {
            "inputs": [[d.namespace, d.name.replace(here, "<dir>")] for d in event.inputs or ()],
            "outputs": [[d.namespace, d.name] for d in event.outputs or ()],
        }
    return out


def compute() -> dict[str, Any]:
    doc: dict[str, Any] = {}
    for name, (project_dir, target) in PROJECTS.items():
        engine = {"tpch_pandas": "pipeloom_pandas"}.get(name)
        if engine is not None and importlib.util.find_spec(engine) is None:
            pytest.skip(f"{engine} is not installed, so {name} cannot compile")
        plan = compile_plan(project_dir, target)
        doc[name] = {
            "airflow_default": airflow_assets(plan, target),
            "airflow_all_placeholders": airflow_assets(plan, target, asset_uri=ALL_PLACEHOLDERS),
            "unit_graph": {u["unit_id"]: u["assets"] for u in unit_graph_dict(plan)["units"]},
            "openlineage": openlineage(plan, project_dir),
            "openlineage_dataset_namespace": openlineage(
                plan, project_dir, dataset_namespace="lake"
            ),
        }
    return doc


def test_dataset_names_match_1_0_0() -> None:
    text = json.dumps(compute(), indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    if os.environ.get("PIPELOOM_UPDATE_GOLDENS"):
        GOLDEN.parent.mkdir(parents=True, exist_ok=True)
        with GOLDEN.open("w", encoding="utf-8", newline="\n") as fh:
            fh.write(text)
        return
    assert text == GOLDEN.read_bytes().decode("utf-8").replace("\r\n", "\n")
