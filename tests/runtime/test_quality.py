"""Expectations and quarantine: the judge, a run on the memory engine, and the shared golden."""

from __future__ import annotations

import io
import json
import shutil
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from rich.console import Console

from pipeloom.api import Project, ProjectOptions
from pipeloom.engine.types import ExpectationSpec, QualityCounts
from pipeloom.events.bus import EventBus
from pipeloom.events.sinks.console import ConsoleSink
from pipeloom.quality import judge
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import JsonTableStore
from pipeloom.testing.recording import RecordingSink
from tests.conftest import FIXTURES

NOW = datetime(2026, 1, 2, tzinfo=UTC)
ORDERS = [
    {"id": 1, "customer": "a@x.com", "amount": 10.0, "status": "paid"},
    {"id": 2, "customer": "b@x.com", "amount": 20.0, "status": "paid"},
    {"id": 3, "customer": None, "amount": 5.0, "status": "paid"},
    {"id": 4, "customer": "d@x.com", "amount": -3.0, "status": "paid"},
    {"id": 5, "customer": "e@x.com", "amount": 150.0, "status": "shipped"},
    {"id": 6, "customer": "f@x.com", "amount": 7.0, "status": "paid"},
    {"id": 7, "customer": "g@x.com", "amount": 8.0, "status": "paid"},
]
KEYS = (
    "unit_id",
    "status",
    "rows_written",
    "rows_dropped",
    "rows_quarantined",
    "expectations",
    "warnings",
)


def reduced(doc: dict[str, Any]) -> list[dict[str, Any]]:
    """The engine-independent part of each results row."""
    out = []
    for r in doc["results"]:
        item = {k: r[k] for k in KEYS}
        item["error"] = None if r["error"] is None else r["error"]["code"]
        out.append(item)
    return out


def memory_project(tmp_path: Path) -> Path:
    shutil.copytree(
        FIXTURES / "quality", tmp_path, ignore=shutil.ignore_patterns("target"), dirs_exist_ok=True
    )
    yml = tmp_path / "pipeloom.yml"
    cfg = {
        "kind": "memory",
        "root": (tmp_path / "store").as_posix(),
        "tables": {f"{tmp_path.as_posix()}/raw/orders": ORDERS},
    }
    text = yml.read_text(encoding="utf-8")
    text = text.replace("engine: {kind: memory}", f"engine: {json.dumps(cfg)}")
    yml.write_text(text, encoding="utf-8", newline="\n")
    src = tmp_path / "models" / "sources.yml"
    swapped = src.read_text(encoding="utf-8").replace("format: csv,", "format: memory,")
    src.write_text(swapped.replace("orders.csv", "orders"), encoding="utf-8", newline="\n")
    return tmp_path


def run(root: Path, target: str, run_id: str = "r1") -> tuple[RunOutcome, RecordingSink]:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    sink = RecordingSink()
    proj = Project(
        ProjectOptions(project_dir=root, target=target, env={"PIPELOOM_USER": "tester"}),
        clock=clock,
        invocation_id="inv",
    )
    with EventBus([(sink, 5)], clock=clock, invocation_id="inv") as bus:
        return proj.run(RunOptions(run_id=run_id, workers=1), bus), sink


def results(root: Path, run_id: str = "r1") -> dict[str, Any]:
    doc: dict[str, Any] = json.loads(
        (root / "target" / "runs" / run_id / "run_results.json").read_bytes()
    )
    return doc


def test_quality_run_results_golden(tmp_path: Path, golden: Callable[[str, str], None]) -> None:
    root = memory_project(tmp_path)
    outcome, sink = run(root, "memory")
    assert (outcome.status, outcome.exit_code) == ("failed", 1)
    text = json.dumps(reduced(results(root)), indent=2, ensure_ascii=True) + "\n"
    golden("run_results/quality.json", text)
    (event,) = [
        e for e in sink.events if e.type == "model.quality_evaluated" and e.model == "orders"
    ]
    assert event.data["rows_dropped"] == 1 and event.data["failures"] == []
    assert event.level.name == "WARNING"
    store = JsonTableStore(root / "store")
    assert len(store["main.orders"]) == 6
    (q,) = store["main.orders__quarantine"]
    assert q["id"] == 4 and q["_pipeloom_failed"] == ["amount_not_negative"]
    assert q["_pipeloom_run_id"] == "r1" and q["_pipeloom_ts"].startswith("2026-01-01")
    assert "main.orders_strict" not in store  # a failed expectation writes nothing


def test_a_failed_expectation_keeps_counts(tmp_path: Path) -> None:
    root = memory_project(tmp_path)
    run(root, "memory")
    strict = next(r for r in results(root)["results"] if r["unit_id"] == "orders_strict")
    assert strict["status"] == "failed" and strict["error"]["code"] == "PLQ301"
    assert strict["expectations"][0]["failed"] == 1


def test_quarantine_replay_with_the_same_run_id_writes_once(tmp_path: Path) -> None:
    root = memory_project(tmp_path)
    run(root, "memory")
    run(root, "memory")
    assert len(JsonTableStore(root / "store")["main.orders__quarantine"]) == 1


def test_warn_console_golden(tmp_path: Path, golden: Callable[[str, str], None]) -> None:
    root = memory_project(tmp_path)
    out = io.StringIO()
    console = Console(file=out, width=110, color_system=None, force_terminal=False)
    clock = FakeClock(datetime(2026, 1, 1, 14, 2, 11, tzinfo=UTC), step=timedelta(0))
    proj = Project(
        ProjectOptions(project_dir=root, target="memory", env={"PIPELOOM_USER": "tester"}),
        clock=clock,
        invocation_id="inv",
    )
    with EventBus([(ConsoleSink(console, tz=UTC), 5)], clock=clock, invocation_id="inv") as bus:
        proj.run(RunOptions(run_id="r1", workers=1), bus)
    warn = "".join(ln + "\n" for ln in out.getvalue().splitlines() if "WARN" in ln)
    golden("events/console_warn.txt", warn)


def _specs() -> list[ExpectationSpec]:
    return [
        ExpectationSpec("neg", "range", "drop", ("a",), {"min": 0}),
        ExpectationSpec("nn", "not_null", "fail", ("a",), {}),
        ExpectationSpec("fresh", "freshness", "fail", ("ts",), {"max_age": 3600}),
        ExpectationSpec("rows", "row_count", "warn", (), {"min": 1}),
    ]


def test_judge_with_zero_rows() -> None:
    counts = QualityCounts(0, {"neg": 0, "nn": 0}, 0, {"fresh": None, "rows": 0})
    report = judge(_specs(), counts, counts, now=NOW, max_drop_ratio=0.1, cached=False)
    assert report.drop_ratio == 0.0
    # all-null freshness fails; an empty table breaks the row_count minimum (a warn)
    assert report.failures == ("fresh",)
    assert [w.name for w in report.warnings] == ["rows"]


def test_judge_fail_and_drop_together_and_max_drop_ratio() -> None:
    row = QualityCounts(10, {"neg": 6, "nn": 2}, 6, {})
    agg = QualityCounts(4, {}, 0, {"fresh": NOW - timedelta(seconds=90), "rows": 4})
    report = judge(_specs(), row, agg, now=NOW, max_drop_ratio=0.5, cached=True, rows_quarantined=6)
    assert report.failures == ("nn", "max_drop_ratio")
    assert report.rows_dropped == 6 and report.rows_quarantined == 6 and report.cached
    assert report.drop_ratio == 0.6
    fresh = next(r for r in report.results if r.name == "fresh")
    assert fresh.failed == 0 and fresh.value == 90.0


def test_judge_freshness_with_a_fake_clock() -> None:
    clock = FakeClock(NOW, step=timedelta(0))
    counts = QualityCounts(1, {}, 0, {"fresh": datetime(2026, 1, 1, 22, 0)})  # naive means UTC
    spec = [ExpectationSpec("fresh", "freshness", "fail", ("ts",), {"max_age": 3600})]
    report = judge(spec, counts, counts, now=clock.now(), max_drop_ratio=None, cached=False)
    assert report.failures == ("fresh",)  # 2 h old
    clock.advance(-3600 * 1.5)
    report = judge(spec, counts, counts, now=clock.now(), max_drop_ratio=None, cached=False)
    assert report.failures == ()
