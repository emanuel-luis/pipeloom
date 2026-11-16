"""tools/marquez_check.py without Marquez: the expectations from a plan and the comparison with
recorded Marquez API payloads."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tools import marquez_check as mc

from pipeloom.api import Project, ProjectOptions
from pipeloom_openlineage.sink import run_uuid, unit_uuid
from tests.runtime.test_runner import shop

SRC = ("file", "/data/orders")
OUT = ("pipeloom.dev", "tpch.orders")
EXP = mc.Expected("p", "r1", {"p.orders": (frozenset({SRC}), frozenset({OUT}))}, frozenset({OUT}))


def ds(pair: tuple[str, str]) -> dict[str, str]:
    return {"namespace": pair[0], "name": pair[1]}


def payloads() -> dict[str, Any]:
    """The shape Marquez 0.50 returns for these paths, trimmed to the keys the check reads."""
    run = {
        "id": unit_uuid("p", "r1", "orders", 1),
        "state": "COMPLETED",
        "facets": {"parent": {"run": {"runId": run_uuid("p", "r1")}, "job": {"name": "p"}}},
    }
    return {
        "/api/v1/namespaces/pipeloom.dev/jobs?limit=1000": {
            "totalCount": 2,
            "jobs": [
                {"name": "p", "namespace": "pipeloom.dev", "inputs": [], "outputs": []},
                {
                    "name": "p.orders",
                    "namespace": "pipeloom.dev",
                    "inputs": [ds(SRC)],
                    "outputs": [ds(OUT)],
                },
            ],
        },
        "/api/v1/namespaces/pipeloom.dev/datasets/tpch.orders": {
            "name": "tpch.orders",
            "fields": [{"name": "o_orderkey", "type": "Int64"}],
            "facets": {
                "dataQualityAssertions": {
                    "assertions": [{"assertion": "not_null", "success": True}]
                }
            },
        },
        "/api/v1/namespaces/pipeloom.dev/jobs/p.orders/runs?limit=100": {"runs": [run]},
    }


def lines(data: dict[str, Any]) -> list[str]:
    return [c.line() for c in mc.checks(EXP, data.__getitem__)]


def test_every_check_passes_on_a_complete_backend() -> None:
    assert lines(payloads()) == [
        "CHECK jobs ok",
        "CHECK lineage_edges ok",
        "CHECK schema_fields ok",
        "CHECK quality_assertions ok",
        "CHECK parent_run_ids ok",
    ]


def test_each_gap_is_a_fail_with_its_detail() -> None:
    data = payloads()
    jobs = data["/api/v1/namespaces/pipeloom.dev/jobs?limit=1000"]["jobs"]
    jobs[0]["name"] = "other"
    jobs[1]["inputs"] = []
    data["/api/v1/namespaces/pipeloom.dev/datasets/tpch.orders"] = {"fields": [], "facets": {}}
    runs = data["/api/v1/namespaces/pipeloom.dev/jobs/p.orders/runs?limit=100"]["runs"]
    runs[0]["facets"] = {}
    assert lines(data) == [
        "CHECK jobs FAIL missing p",
        "CHECK lineage_edges FAIL differ for p.orders",
        "CHECK schema_fields FAIL no fields on tpch.orders",
        "CHECK quality_assertions FAIL no assertions on tpch.orders",
        "CHECK parent_run_ids FAIL wrong or missing on p.orders",
    ]


def test_expected_follows_the_manifest(tmp_path: Path) -> None:
    root = shop(tmp_path)
    plan = Project(ProjectOptions(project_dir=root, target="ci", env={})).compile().value
    assert plan is not None
    exp = mc.expected(plan, root, "r1")
    assert "shop" in exp.jobs and "shop.stg_orders" in exp.jobs
    inputs, outputs = exp.edges["shop.fct_orders"]
    assert sorted(n.rsplit(".", 1)[-1] for _, n in inputs) == [
        "stg_customers",
        "stg_orders",
        "stg_payments",
    ]
    assert len(outputs) == 1
    assert exp.quality <= {o for _, outs in exp.edges.values() for o in outs}


def test_the_sink_block_is_block_yaml() -> None:
    assert "transport:\n        type: http\n        url: http://h:1\n" in mc.sink_block(
        "http://h:1"
    )
