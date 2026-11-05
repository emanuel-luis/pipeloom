"""A shop project end to end on Spark: Delta tables, the `table` state store, expectations with
drop and quarantine, an intermediate model, token replay and cancellation.

The engine and the state store find the test module's session (mode auto means active), so the
whole run shares one JVM with the fixture.
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.runtime.control import RunControl
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink

TARGETS = (
    "targets:\n"
    "  spark:\n"
    "    engine: {kind: spark}\n"
    "    catalog: {kind: delta_path, root: target/warehouse}\n"
    "    state: {kind: table, path: .pipeloom/state}\n"
    "    naming: {kind: none}\n"
    "  connect:\n"
    "    engine: {kind: spark}\n"
    "    catalog: {kind: spark_catalog}\n"
    "    state: {kind: table, table: default.pipeloom_state}\n"
    "    naming: {kind: none}\n"
)
SHOP = {
    "pipeloom.yml": "config_version: 1\nname: shop\npython_paths: [src]\n"
    "defaults:\n  models: {materialized: table}\n" + TARGETS,
    "models/sources.yml": (
        "sources:\n"
        "  - {name: raw_customers, format: csv, location: raw/customers.csv}\n"
        "  - {name: raw_orders, format: csv, location: raw/orders.csv}\n"
    ),
    "models/models.yml": (
        "models:\n"
        "  - name: stg_customers\n"
        "    sql: select id, lower(email) as email from raw_customers\n"
        "    inputs: {raw_customers: {source: raw_customers}}\n"
        "  - name: stg_orders\n"
        "    materialized: intermediate\n"
        "    sql: select id, customer_id, amount from raw_orders\n"
        "    inputs: {raw_orders: {source: raw_orders}}\n"
        "  - name: fct_orders\n"
        "    write: {strategy: append}\n"
        "    sql: >-\n"
        "      select o.id, o.customer_id, c.email, o.amount\n"
        "      from orders o left join customers c on o.customer_id = c.id\n"
        "    inputs: {orders: {ref: stg_orders}, customers: {ref: stg_customers}}\n"
        "    quality: {quarantine: true, max_drop_ratio: 0.5}\n"
        "    expectations:\n"
        "      - {name: amount_not_negative, check: {range: {column: amount, min: 0}},"
        " on_violation: drop}\n"
        "      - {name: email_set, check: {not_null: [email]}, on_violation: warn}\n"
        "      - {name: amount_small, expr: 'amount < 100', on_violation: warn}\n"
        "      - {name: id_unique, check: {unique: [id]}, on_violation: fail}\n"
        "  - name: rpt_revenue\n"
        "    sql: select customer_id, sum(amount) as revenue from fct group by customer_id\n"
        "    inputs: {fct: {ref: fct_orders}}\n"
    ),
    "raw/customers.csv": "id,email\n1,A@x.com\n2,b@X.com\n",
    "raw/orders.csv": "id,customer_id,amount\n10,1,5.0\n11,2,7.5\n12,1,2.5\n13,3,-1.0\n",
}
SLOW = {
    "pipeloom.yml": SHOP["pipeloom.yml"],
    "models/sources.yml": "sources:\n  - {name: raw_orders, format: csv, location: raw/orders.csv}\n",
    "models/models.yml": (
        "models:\n  - name: slow\n    python: slowpkg:slow\n"
        "    inputs: {raw: {source: raw_orders}}\n"
    ),
    "raw/orders.csv": SHOP["raw/orders.csv"],
    "src/slowpkg/__init__.py": (
        "def slow(ctx, raw):\n"
        "    # runs until interrupted\n"
        "    return raw.sparkSession.range(0, 10**13).selectExpr('sum(sqrt(id)) as s')\n"
    ),
}


def project(root: Path, files: Mapping[str, str]) -> Path:
    for rel, text in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_bytes(text.encode("utf-8"))
    return root


def run(
    root: Path,
    run_id: str,
    target: str = "spark",
    control: RunControl | None = None,
    sink: RecordingSink | None = None,
) -> tuple[RunOutcome, RecordingSink]:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    sink = sink or RecordingSink()
    proj = Project(
        ProjectOptions(project_dir=root, target=target, env={}), clock=clock, invocation_id="inv"
    )
    opts = RunOptions(run_id=run_id, workers=2, control=control, cancel_grace=1)
    with EventBus([(sink, 5)], clock=clock, invocation_id="inv") as bus:
        return proj.run(opts, bus), sink


def rows(spark: Any, frame: Any, order: str) -> list[dict[str, Any]]:
    return [r.asDict() for r in frame.orderBy(order).collect()]


def delta(spark: Any, root: Path, name: str) -> Any:
    (place,) = [p for p in (root / "target" / "warehouse").rglob(name) if p.is_dir()]
    return spark.read.format("delta").load(place.as_posix())


def commits(root: Path) -> list[str]:
    return sorted(p.as_posix() for p in (root / "target").rglob("_delta_log/*.json"))


def results(root: Path, run_id: str) -> dict[str, dict[str, Any]]:
    doc = json.loads((root / "target" / "runs" / run_id / "run_results.json").read_bytes())
    return {r["unit_id"]: r for r in doc["results"]}


def check_shop(spark: Any, fct: Any, rpt: Any, quarantine: Any) -> None:
    assert rows(spark, fct, "id") == [
        {"id": 10, "customer_id": 1, "email": "a@x.com", "amount": 5.0},
        {"id": 11, "customer_id": 2, "email": "b@x.com", "amount": 7.5},
        {"id": 12, "customer_id": 1, "email": "a@x.com", "amount": 2.5},
    ]
    assert rows(spark, rpt, "customer_id") == [
        {"customer_id": 1, "revenue": 7.5},
        {"customer_id": 2, "revenue": 7.5},
    ]
    (q,) = quarantine.select("id", "_pipeloom_run_id", "_pipeloom_failed").collect()
    assert (q.id, q._pipeloom_run_id, q._pipeloom_failed) == (13, "r1", ["amount_not_negative"])


@pytest.mark.spark
@pytest.mark.timeout(600)
def test_shop_runs_and_a_rerun_with_the_same_run_id_writes_nothing(
    classic_spark: Any, tmp_path: Path
) -> None:
    root = project(tmp_path, SHOP)
    outcome, sink = run(root, "r1")
    assert (outcome.status, outcome.exit_code) == ("succeeded", 0), outcome.diagnostics
    got = results(root, "r1")
    assert {u: r["status"] for u, r in got.items()} == dict.fromkeys(
        ("stg_customers", "stg_orders", "fct_orders", "rpt_revenue"), "succeeded"
    )
    assert got["fct_orders"]["rows_dropped"] == 1 and got["fct_orders"]["rows_quarantined"] == 1
    assert {r["engine"] for r in got.values()} == {"spark"}
    check_shop(
        classic_spark,
        delta(classic_spark, root, "fct_orders"),
        delta(classic_spark, root, "rpt_revenue"),
        delta(classic_spark, root, "fct_orders__quarantine"),
    )
    assert (root / ".pipeloom" / "state" / "_delta_log").is_dir()
    before = commits(root)

    again, sink = run(root, "r1")
    assert again.exit_code == 0, again.diagnostics
    written = [e for e in sink.events if e.type == "model.written"]
    assert written and all(e.data["skipped"] for e in written)
    assert commits(root) == before  # no new Delta commit anywhere: every write replayed


@pytest.mark.spark
@pytest.mark.timeout(300)
def test_cancel_stops_a_slow_model(classic_spark: Any, tmp_path: Path) -> None:
    """The run ends cancelled and no Spark job is left running. Classic job tags are per
    thread, so the removal of the unit's tag is covered on one thread by
    test_engine_classic.py::test_unit_scope_adds_and_removes_its_tag, not here."""
    root = project(tmp_path, SLOW)
    control = RunControl()
    box: list[tuple[RunOutcome, RecordingSink]] = []
    sink = RecordingSink()
    worker = threading.Thread(
        target=lambda: box.append(run(root, "c1", control=control, sink=sink))
    )
    worker.start()
    tracker = classic_spark.sparkContext.statusTracker()
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline and not any(
        e.type == "unit.started" and e.unit_id == "slow" for e in list(sink.events)
    ):
        time.sleep(0.2)
    time.sleep(3)  # the read is done and the endless write job runs
    assert tracker.getActiveJobsIds(), "the slow job never started"
    control.request_stop()
    worker.join(timeout=120)
    assert not worker.is_alive()
    outcome, _ = box[0]
    assert outcome.status == "cancelled"
    assert results(root, "c1")["slow"]["status"] == "cancelled"
    deadline = time.monotonic() + 30
    while tracker.getActiveJobsIds() and time.monotonic() < deadline:
        time.sleep(0.2)
    assert not tracker.getActiveJobsIds()  # interrupted, not left running


@pytest.mark.spark_connect
@pytest.mark.timeout(600)
def test_shop_runs_on_connect_with_the_session_catalog(connect_spark: Any, tmp_path: Path) -> None:
    root = project(tmp_path, SHOP)
    outcome, _ = run(root, "r1", target="connect")
    assert (outcome.status, outcome.exit_code) == ("succeeded", 0), outcome.diagnostics
    check_shop(
        connect_spark,
        connect_spark.read.table("main.fct_orders"),
        connect_spark.read.table("main.rpt_revenue"),
        connect_spark.read.table("main.fct_orders__quarantine"),
    )
    assert connect_spark.read.table("default.pipeloom_state").where("kind = 'run'").count() == 1
