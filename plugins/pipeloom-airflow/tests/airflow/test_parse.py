"""The DAG templates parse in a real DagBag, in a subprocess that cannot reach any network or
database, within the parse budgets. Airflow parses a DAG file before every task, so the budget
is also the start-up cost of each task."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.plan.manifest import write_manifest
from pipeloom_airflow import naming

HERE = Path(__file__).resolve().parent
Plan = tuple[Any, Any]
SCALE = float(os.environ.get("PIPELOOM_TEST_PARSE_BUDGET_SCALE", "1"))
"""Parse budgets times this; the WSL script sets 3 because /mnt/<drive> imports are slow."""


@pytest.fixture
def wide(tmp_path: Path) -> tuple[Path, Path]:
    project = tmp_path / "wide"
    (project / "models").mkdir(parents=True)
    (project / "pipeloom.yml").write_text(
        "config_version: 1\nname: wide\nengine: {kind: memory}\ncatalog: {kind: memory}\n"
        "targets:\n  ci:\n    naming: {kind: none}\n",
        encoding="utf-8",
    )
    lines = ["models:"] + [f"  - {{name: m{i:03d}, sql: select 1 as x}}" for i in range(500)]
    (project / "models" / "m.yml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    outcome = Project(ProjectOptions(project_dir=project, target="ci", env={})).compile()
    assert outcome.value is not None
    manifest = project / "manifest.json"
    write_manifest(outcome.value, manifest, stable=True)
    return project, manifest


def parse(
    tmp_path: Path,
    dag_file: str,
    af_project: tuple[Path, Path, str],
    wide: tuple[Path, Path],
    extra: dict[str, str] | None = None,
) -> dict[str, Any]:
    home = tmp_path / "home"
    out = tmp_path / "parsed.json"
    env = {
        "PATH": os.environ["PATH"],
        "HOME": str(tmp_path),
        "AIRFLOW_HOME": str(home),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__CORE__DAGS_FOLDER": str(HERE / "dags"),
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{home}/never.db",
        "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT": "120",
        "PYTHONHASHSEED": "0",
        "PIPELOOM_AF_PROJECT": af_project[0].as_posix(),
        "PIPELOOM_AF_MANIFEST": af_project[1].as_posix(),
        "PIPELOOM_AF_CACHE": (tmp_path / "cache").as_posix(),
        "PIPELOOM_AF_WIDE_PROJECT": wide[0].as_posix(),
        "PIPELOOM_AF_WIDE_MANIFEST": wide[1].as_posix(),
        **(extra or {}),
    }
    done = subprocess.run(
        [sys.executable, "-I", str(HERE / "_parse_helper.py"), str(HERE / "dags" / dag_file), out],
        env=env,
        timeout=180,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode == 0, done.stderr[-4000:]
    assert not (home / "never.db").exists()
    result: dict[str, Any] = json.loads(out.read_text(encoding="utf-8"))
    assert result["import_errors"] == {}, result["import_errors"]
    return result


@pytest.mark.timeout(240)
@pytest.mark.parametrize("dag_file", ["shop_local.py", "shop_venv.py"])
def test_a_pipeloom_dag_parses_into_the_unit_graph(
    tmp_path: Path, af_project: Any, af_plan: Plan, wide: Any, dag_file: str
) -> None:
    result = parse(tmp_path, dag_file, af_project, wide)
    assert result["parse_s"] < 2.0 * SCALE, result["parse_s"]
    (dag,) = result["dags"].values()
    plan = af_plan[0]
    tasks = dag["tasks"]
    assert set(tasks) == {t.task_id for t in plan.tasks} | {naming.FINALIZE_ID}
    assert dag["serialized_tasks"] == len(tasks)
    assert "full_refresh" in dag["params"]
    for t in plan.tasks:
        assert tasks[t.task_id]["class"] == "PipeloomRunOperator"
        assert tasks[t.task_id]["upstream"] == list(t.upstream)
        assert tasks[t.task_id]["retries"] == t.retries
    finalize = tasks[naming.FINALIZE_ID]
    assert finalize["class"] == "PipeloomFinalizeOperator"
    assert finalize["trigger_rule"] == "all_done" and finalize["retries"] == 1
    assert finalize["upstream"] == sorted(plan.leaves)
    by_model = {t.models[0]: t.task_id for t in plan.tasks}
    assert tasks[by_model["child"]]["upstream"] == [by_model["base"]]
    if dag_file == "shop_local.py":
        assert tasks[by_model["base"]]["outlets"] == ["pipeloom://afproj/dev/main.base"]


@pytest.mark.timeout(240)
def test_the_kubernetes_dag_parses_into_pod_operators(
    tmp_path: Path, af_project: Any, af_plan: Plan, wide: Any
) -> None:
    result = parse(tmp_path, "shop_k8s.py", af_project, wide)
    assert result["parse_s"] < 4.0 * SCALE, result["parse_s"]
    tasks = result["dags"]["shop_k8s"]["tasks"]
    plan = af_plan[0]
    assert set(tasks) == {t.task_id for t in plan.tasks} | {naming.FINALIZE_ID}
    for t in plan.tasks:
        assert tasks[t.task_id]["class"] == "PipeloomKubernetesOperator"
        assert tasks[t.task_id]["upstream"] == list(t.upstream)
        assert tasks[t.task_id]["retries"] == t.retries
    finalize = tasks[naming.FINALIZE_ID]
    assert finalize["class"] == "PipeloomKubernetesFinalizeOperator"
    assert finalize["trigger_rule"] == "all_done" and finalize["retries"] == 1
    assert finalize["upstream"] == sorted(plan.leaves)


@pytest.mark.timeout(240)
def test_the_databricks_dag_parses_into_launch_sensors_and_finalize(
    tmp_path: Path, af_project: Any, dbx_project: Any, wide: Any
) -> None:
    extra = {
        "PIPELOOM_AF_DBX_PROJECT": dbx_project[0].as_posix(),
        "PIPELOOM_AF_DBX_MANIFEST": dbx_project[1].as_posix(),
    }
    result = parse(tmp_path, "shop_databricks.py", af_project, wide, extra)
    assert result["parse_s"] < 2.0 * SCALE, result["parse_s"]
    tasks = result["dags"]["shop_databricks"]["tasks"]
    graph = {
        k: (v["class"], v["upstream"], v["trigger_rule"], v["retries"]) for k, v in tasks.items()
    }
    launch = naming.LAUNCH_ID
    assert graph == {
        launch: ("PipeloomDatabricksRunOperator", [], "all_success", 2),
        "a": ("PipeloomDatabricksTaskSensor", [launch], "all_success", 0),
        "b": ("PipeloomDatabricksTaskSensor", ["a"], "all_success", 0),
        "c": ("PipeloomDatabricksTaskSensor", [launch], "all_success", 0),
        naming.FINALIZE_ID: ("PipeloomDatabricksTaskSensor", ["b", "c", launch], "all_done", 0),
    }


@pytest.mark.timeout(240)
def test_a_task_group_nests_the_units_between_other_tasks(
    tmp_path: Path, af_project: Any, af_plan: Plan, wide: Any
) -> None:
    result = parse(tmp_path, "shop_group.py", af_project, wide)
    tasks = result["dags"]["shop_group"]["tasks"]
    plan = af_plan[0]
    units = {f"shop.untagged.{t.task_id}" for t in plan.tasks}
    assert set(tasks) == units | {"start", "end", f"shop.{naming.FINALIZE_ID}"}
    for task_id in units:
        if task_id.endswith(next(t.task_id for t in plan.tasks if t.models == ("base",))):
            assert tasks[task_id]["upstream"] == ["start"]
    assert tasks["end"]["upstream"] == [f"shop.{naming.FINALIZE_ID}"]


@pytest.mark.timeout(240)
def test_500_units_parse_within_the_budget(tmp_path: Path, af_project: Any, wide: Any) -> None:
    result = parse(tmp_path, "wide_500.py", af_project, wide)
    assert result["parse_s"] < 8.0 * SCALE, result["parse_s"]
    tasks = result["dags"]["wide_500"]["tasks"]
    assert len(tasks) == 501
    assert len(tasks[naming.FINALIZE_ID]["upstream"]) == 500
