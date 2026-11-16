"""No duplicate OpenLineage events under Airflow (ADR-0046): a unit task with the openlineage sink
configured in the project. The sink in the child stays silent and the operator's facets report
the unit once. The sink writes through the client's file transport, so nothing leaves the host."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from airflow.providers.openlineage.extractors import OperatorLineage

from pipeloom_airflow import _compat
from pipeloom_airflow.config import ExecutionConfig, ProjectConfig, RenderConfig
from pipeloom_airflow.dag import _plan, build_tasks
from pipeloom_airflow.testing.context import FakeTaskInstance, make_context

AFTER = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)
START = datetime(2026, 1, 1)


def with_sink(project: Path, log: Path, inside_airflow: str) -> None:
    sinks = f"""events:
  sinks:
    - kind: openlineage
      inside_airflow: {inside_airflow}
      transport:
        type: file
        log_file_path: {log.as_posix()}
        append: true
"""
    yml = project / "pipeloom.yml"
    yml.write_text(yml.read_text(encoding="utf-8") + sinks, encoding="utf-8")


def run_base(af_project: tuple[Path, Path, str]) -> Any:
    """Build the DAG's tasks, run the task of the model `base` and return its operator."""
    project, manifest, _ = af_project
    plan, command, _ = _plan(
        ProjectConfig(project.as_posix(), manifest_path=manifest.as_posix()),
        "dev",
        RenderConfig(),
        ExecutionConfig(),
        {},
    )
    task = next(t for t in plan.tasks if t.models == ("base",))
    with _compat.DAG(dag_id="d", schedule=None, start_date=START):
        ops = build_tasks(plan, ExecutionConfig(), command, group_id=None, operator_args={})
    op = ops[task.task_id]
    ti = FakeTaskInstance(dag_id="d", run_id="manual__1", task_id=op.task_id)
    op.execute(make_context(ti, run_after=AFTER, params={}))
    return op


def sink_events(log: Path) -> list[dict[str, Any]]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines() if line]


def test_the_sink_is_silent_and_the_operator_reports_the_unit_once(
    af_project: tuple[Path, Path, str], tmp_path: Path
) -> None:
    log = tmp_path / "ol.jsonl"
    with_sink(af_project[0], log, "skip")
    op = run_base(af_project)
    assert sink_events(log) == []

    start = op.get_openlineage_facets_on_start()
    assert isinstance(start, OperatorLineage)
    lineage = op.get_openlineage_facets_on_complete(None)
    assert isinstance(lineage, OperatorLineage)
    [out] = lineage.outputs
    assert out.name.endswith("base")
    assert out.outputFacets["outputStatistics"].rowCount == 3
    assert [f.name for f in out.facets["schema"].fields] == ["id"]
    assert "parent" not in lineage.run_facets and "jobType" not in lineage.job_facets


def test_emit_makes_the_sink_report_too(af_project: tuple[Path, Path, str], tmp_path: Path) -> None:
    log = tmp_path / "ol.jsonl"
    with_sink(af_project[0], log, "emit")
    run_base(af_project)
    jobs = [(e["job"]["name"], e["eventType"]) for e in sink_events(log)]
    assert ("afproj.base", "COMPLETE") in jobs
