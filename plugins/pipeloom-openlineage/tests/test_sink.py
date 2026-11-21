"""The OpenLineage sink, offline: a recording transport stands in for the backend."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunEvent, RunState
from openlineage.client.serde import Serde
from openlineage.client.transport import Transport
from tests.runtime.test_runner import shop

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus, PlanInfo
from pipeloom.events.model import (
    ModelQualityEvaluated,
    ModelWritten,
    RunCompleted,
    UnitStarted,
    UnitSucceeded,
)
from pipeloom.graph.selectors import Selection
from pipeloom.plan.identity import location_dataset
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink
from pipeloom_openlineage.sink import OpenLineageSink, run_uuid, unit_lineage, unit_uuid
from pipeloom_openlineage.spec import SPEC, OpenLineageOptions

CLOCK = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
AIRFLOW = {"orchestrator_id.airflow_dag_id": "shop"}
UNITS = ("stg_customers", "stg_orders", "stg_payments", "dim_customers", "fct_orders")


class Recording(Transport):
    kind = "recording"

    def __init__(self) -> None:
        self.events: list[RunEvent] = []

    def emit(self, event: Any) -> None:
        assert isinstance(event, RunEvent)
        Serde.to_json(event)  # every event must serialize
        self.events.append(event)


def make_sink(**options: Any) -> tuple[OpenLineageSink, Recording]:
    transport = Recording()
    return OpenLineageSink(client=OpenLineageClient(transport=transport), **options), transport


def run(
    root: Path, run_id: str = "run1", context: Mapping[str, str] | None = None, **options: Any
) -> tuple[RunOutcome, Recording]:
    sink, transport = make_sink(**options)
    project = Project(
        ProjectOptions(project_dir=root, target="ci", env={"PIPELOOM_USER": "tester"}),
        clock=CLOCK,
        invocation_id="inv",
    )
    with EventBus([(sink, 1)], clock=CLOCK, invocation_id="inv", context=context) as bus:
        outcome = project.run(RunOptions(Selection(()), run_id=run_id, workers=1), bus)
    return outcome, transport


def summary(events: list[RunEvent]) -> list[tuple[str, str]]:
    return [(e.job.name, e.eventType.value if e.eventType else "") for e in events]


def by_job(events: list[RunEvent], name: str, state: RunState) -> RunEvent:
    return next(e for e in events if e.job.name == name and e.eventType is state)


def facet(facets: Mapping[str, Any] | None, key: str) -> Any:
    """A facet by key, typed loosely: the dicts are typed with the facet base classes."""
    assert facets is not None
    return facets[key]


def test_a_successful_run(tmp_path: Path) -> None:
    root = shop(tmp_path)
    outcome, transport = run(root)
    assert outcome.exit_code == 0
    events = transport.events
    got = summary(events)
    assert got[0] == ("shop", "START") and got[-1] == ("shop", "COMPLETE")
    for unit in (*UNITS, "rpt_revenue"):
        assert got.index((f"shop.{unit}", "START")) < got.index((f"shop.{unit}", "COMPLETE"))
    assert {e.job.namespace for e in events} == {"pipeloom.ci"}

    parent = by_job(events, "shop", RunState.START)
    assert parent.run.runId == run_uuid("shop", "run1")
    assert facet(parent.job.facets, "jobType").jobType == "JOB"

    done = by_job(events, "shop.stg_orders", RunState.COMPLETE)
    assert done.run.runId == unit_uuid("shop", "run1", "stg_orders", 1)
    assert facet(done.run.facets, "parent").run.runId == run_uuid("shop", "run1")
    assert facet(done.run.facets, "parent").job.name == "shop"
    assert done.inputs is not None and done.outputs is not None
    [source] = done.inputs
    assert (source.namespace, source.name) == ("file", f"{root.resolve().as_posix()}/raw/orders")
    [out] = done.outputs
    assert out.name.endswith("stg_orders") and out.namespace == "pipeloom.ci"
    fields = facet(out.facets, "schema").fields
    assert [f.name for f in fields] == ["id", "customer_id", "amount"]
    assert facet(out.outputFacets, "outputStatistics").rowCount == 3
    location = facet(done.job.facets, "sourceCodeLocation")
    assert (location.type, location.path) == ("file", "models/staging/stg_orders.py")


def test_an_ephemeral_model_passes_its_inputs_to_the_unit(tmp_path: Path) -> None:
    _, transport = run(shop(tmp_path))
    fct = by_job(transport.events, "shop.fct_orders", RunState.START)
    assert fct.inputs is not None
    names = sorted(i.name.rsplit(".", 1)[-1] for i in fct.inputs)
    assert names == ["stg_customers", "stg_orders", "stg_payments"]
    assert facet(fct.job.facets, "jobType").jobType == "MODEL"


def test_a_failed_run(tmp_path: Path) -> None:
    root = shop(tmp_path)
    fct = root / "models" / "marts" / "fct_orders.py"
    fct.write_text(
        fct.read_text(encoding="utf-8").replace(
            "    return enriched\n", "    raise ValueError('boom')\n"
        ),
        encoding="utf-8",
    )
    outcome, transport = run(root)
    assert outcome.exit_code == 1
    events = transport.events
    failed = by_job(events, "shop.fct_orders", RunState.FAIL)
    error = facet(failed.run.facets, "errorMessage")
    assert "boom" in error.message and error.programmingLanguage == "python"
    assert "ValueError" in (error.stackTrace or "")
    assert not [e for e in events if e.job.name == "shop.rpt_revenue"]  # skipped: never started
    end = events[-1]
    assert (end.job.name, end.eventType) == ("shop", RunState.FAIL)
    assert "fct_orders" in facet(end.run.facets, "errorMessage").message


def test_ids_are_stable_across_invocations(tmp_path: Path) -> None:
    _, first = run(shop(tmp_path / "a"), run_id="same")
    _, second = run(shop(tmp_path / "b"), run_id="same")
    ids = [(e.job.name, e.run.runId) for e in first.events]
    assert ids == [(e.job.name, e.run.runId) for e in second.events]
    assert all(uuid.UUID(i).version == 5 for _, i in ids)


def test_the_project_scopes_custom_run_ids() -> None:
    assert run_uuid("a", "nightly") != run_uuid("b", "nightly")
    assert unit_uuid("a", "nightly", "m", 1) != unit_uuid("b", "nightly", "m", 1)


def test_silent_inside_airflow_unless_told(tmp_path: Path) -> None:
    _, skipped = run(shop(tmp_path / "a"), context=AIRFLOW)
    assert skipped.events == []
    _, emitted = run(shop(tmp_path / "b"), context=AIRFLOW, inside_airflow="emit")
    assert summary(emitted.events)[0] == ("shop", "START")


MANIFEST: dict[str, Any] = {
    "metadata": {"project": "p", "target": "t"},
    "nodes": {
        "model.p.m": {
            "file": "models/m.yml",
            "line": 3,
            "inputs": {"s": "source.p.s"},
            "relation": {"catalog": None, "schema": "main", "name": "m"},
            "columns": [{"name": "id", "dtype": "int64", "nullable": False}],
        }
    },
    "sources": {"source.p.s": {"location": "s3://bucket/raw/s", "table": None}},
    "units": {"m": ["model.p.m"]},
}


def emit_unit(bus: EventBus) -> None:
    bus.emit(
        UnitStarted(materialization="table", executor="t", queue_wait_ms=0), unit_id="m", attempt=2
    )
    bus.emit(
        ModelQualityEvaluated(
            total_rows=2,
            results=[
                {"name": "ids", "kind": "not_null", "action": "fail", "passed": 2, "failed": 0},
                {"name": "pos", "kind": "expr", "action": "warn", "passed": 1, "failed": 1},
            ],
            rows_dropped=0,
            rows_quarantined=0,
            drop_ratio=0.0,
            cached=False,
            failures=[],
        ),
        unit_id="m",
    )
    bus.emit(
        ModelWritten(
            relation="main.m",
            materialization="table",
            strategy="overwrite",
            rows_written=2,
            rows_inserted=2,
            bytes_written=10,
            duration_ms=0,
        ),
        unit_id="m",
    )
    bus.emit(
        UnitSucceeded(
            materialization="table",
            strategy="overwrite",
            relation="main.m",
            rows_written=2,
            rows_inserted=2,
            bytes_written=10,
            duration_ms=0,
        ),
        unit_id="m",
        attempt=2,
    )


@pytest.mark.parametrize(
    ("location", "expected"),
    [
        ("s3://bucket/raw/s", ("s3://bucket", "raw/s")),
        ("gs://bucket/a/b", ("gs://bucket", "a/b")),
        (
            "abfss://box@acct.dfs.core.windows.net/p/q",
            ("abfss://box@acct.dfs.core.windows.net", "p/q"),
        ),
        ("file:///data/x", ("file", "/data/x")),
        ("file:/data/x", ("file", "/data/x")),
        ("dbfs:/Volumes/c/s/v/x", ("dbfs", "/Volumes/c/s/v/x")),
        ("/abs/x", ("file", "/abs/x")),
        ("rel/x", ("file", "/proj/rel/x")),
    ],
)
def test_location_follows_the_openlineage_naming(location: str, expected: tuple[str, str]) -> None:
    assert location_dataset(location, Path("/proj")) == expected


def test_quality_contract_schema_and_naming_from_the_manifest() -> None:
    sink, transport = make_sink(namespace="{project}-{target}", dataset_namespace="warehouse")
    with EventBus([(sink, 1)], clock=CLOCK, invocation_id="i", run_id="r") as bus:
        bus.plan_ready(PlanInfo(MANIFEST, Path("/p")))
        emit_unit(bus)
    start, done = transport.events
    assert done.run.runId == unit_uuid("p", "r", "m", 2)
    assert (done.job.namespace, done.job.name) == ("p-t", "p.m")
    assert done.inputs is not None and done.outputs is not None
    assert [(i.namespace, i.name) for i in done.inputs] == [("s3://bucket", "raw/s")]
    [out] = done.outputs
    assert (out.namespace, out.name) == ("warehouse", "main.m")
    fields = facet(out.facets, "schema").fields
    assert [f.name for f in fields] == ["id"]  # from the contract: the event had no columns
    assertions = facet(out.facets, "dataQualityAssertions").assertions
    assert [(a.name, a.assertion, a.success) for a in assertions] == [
        ("ids", "not_null", True),
        ("pos", "expr", False),
    ]
    assert facet(done.job.facets, "sourceCodeLocation").url == "models/m.yml#L3"
    assert start.outputs is not None and "dataQualityAssertions" not in (
        start.outputs[0].facets or {}
    )


def test_unit_lineage_replays_a_unit_or_falls_back_to_the_manifest() -> None:
    recording = RecordingSink()
    with EventBus([(recording, 1)], clock=CLOCK, invocation_id="i", run_id="r") as bus:
        emit_unit(bus)
    done = unit_lineage(MANIFEST, Path("/p"), "m", recording.events)
    assert done.eventType is RunState.COMPLETE
    assert done.outputs is not None
    assert "dataQualityAssertions" in (done.outputs[0].facets or {})

    static = unit_lineage(MANIFEST, Path("/p"), "m")
    assert static.eventType is RunState.START and static.job.name == "p.m"
    assert static.inputs is not None and static.outputs is not None
    assert [i.name for i in static.inputs] == ["raw/s"]
    assert [o.name for o in static.outputs] == ["main.m"]


def test_a_replayed_finalize_reports_nothing() -> None:
    sink, transport = make_sink()
    with EventBus([(sink, 1)], clock=CLOCK, invocation_id="i", run_id="r") as bus:
        bus.emit(
            RunCompleted(
                status="failed",
                exit_code=1,
                duration_ms=0,
                counts={},
                rows_written=0,
                bytes_written=0,
                failures=[],
                artifacts=[],
                replay=True,
            )
        )
    assert transport.events == []


def test_the_spec_builds_a_sink_that_waits_for_secrets() -> None:
    opts = OpenLineageOptions(transport={"type": "http", "url": "${secret:env/OL_URL}"})
    sink = SPEC.factory(opts)
    assert isinstance(sink, OpenLineageSink)
    with EventBus([(sink, 1)], clock=CLOCK, invocation_id="i", run_id="r") as bus:
        bus.plan_ready(PlanInfo(MANIFEST, Path("/p")))
        emit_unit(bus)  # no client is built while the URL is a reference
    assert repr(sink) == "OpenLineageSink()"
