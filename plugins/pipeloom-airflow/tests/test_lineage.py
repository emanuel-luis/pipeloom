"""The OpenLineage facets of a unit task, without Airflow: the parts the provider gets."""

from __future__ import annotations

import json
import sys
import types
from typing import Any

import pytest

from pipeloom_airflow.lineage import OpenLineageMixin, child_event, unit_facets
from pipeloom_airflow.load import LoadedPlan

LINE = {
    "schema": "pipeloom.event/1",
    "id": "e1",
    "run_id": "r",
    "invocation_id": "i",
    "seq": 1,
    "ts": "2026-01-01T00:00:00.000000Z",
    "type": "unit.succeeded",
    "level": "info",
    "unit_id": "stg_orders",
    "attempt": 1,
    "data": {"rows_written": 3, "bytes_written": 10},
}


def test_child_event_reads_only_pipeloom_events() -> None:
    event = child_event(json.dumps(LINE) + "\n")
    assert event is not None and event.type == "unit.succeeded"
    assert child_event("plain text") is None
    assert child_event('{"schema": "other/1"}') is None
    assert child_event(json.dumps({**LINE, "seq": "x"})) is None


def test_static_facets_come_from_the_manifest(shop: LoadedPlan) -> None:
    found = unit_facets(shop.plan, "/p", "stg_orders", ())
    assert found is not None
    inputs, outputs, run_facets, job_facets = found
    assert [i.name for i in inputs] == ["/p/raw/orders"]
    assert [o.name.rsplit(".", 1)[-1] for o in outputs] == ["stg_orders"]
    assert "parent" not in run_facets  # the provider's parent is the DAG run
    assert set(job_facets) == {"sourceCodeLocation"}


def test_hooks_return_empty_lineage_without_pipeloom_openlineage(
    shop: LoadedPlan, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `_compat` needs Airflow, so a stand-in replaces it: it records what the mixin asks for and
    # returns None when told the provider is missing, like the real `operator_lineage`.
    provider = {"installed": True}
    calls: list[tuple[Any, ...]] = []

    def operator_lineage(*args: Any) -> Any:
        calls.append(args)
        return args if provider["installed"] else None

    compat = types.ModuleType("pipeloom_airflow._compat")
    compat.operator_lineage = operator_lineage  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pipeloom_airflow._compat", compat)
    monkeypatch.setitem(sys.modules, "pipeloom_openlineage.sink", None)  # ImportError on import
    hooks = OpenLineageMixin()
    hooks.unit_id = "stg_orders"
    hooks.lineage_plan = shop.plan
    assert hooks.get_openlineage_facets_on_start() == ([], [], {}, {})
    provider["installed"] = False
    assert hooks.get_openlineage_facets_on_start() is None
    assert len(calls) == 2


def test_replayed_events_add_output_statistics(shop: LoadedPlan) -> None:
    started = child_event(json.dumps({**LINE, "type": "unit.started", "data": {}}))
    done = child_event(json.dumps(LINE))
    assert started is not None and done is not None
    found = unit_facets(shop.plan, "/p", "stg_orders", [started, done])
    assert found is not None
    [out] = found[1]
    assert out.outputFacets is not None
    assert out.outputFacets["outputStatistics"].rowCount == 3
