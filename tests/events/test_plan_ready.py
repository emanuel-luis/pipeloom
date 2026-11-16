"""The optional `plan_ready` sink hook and the additive `model.written.columns` field."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus, PlanInfo
from pipeloom.events.model import Event, ModelWritten, RunCompiled
from pipeloom.events.sinks.buffered import BufferedSink
from pipeloom.graph.selectors import Selection
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink
from tests.runtime.test_runner import run_project, shop

CLOCK = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
INFO = PlanInfo({"schema": "pipeloom.manifest/1"}, Path("/p"))


class PlanSink(RecordingSink):
    """Records events and plans in one list, so the order between them is visible."""

    def __init__(self, fail_plan: bool = False) -> None:
        super().__init__()
        self.seen: list[Event | PlanInfo] = []
        self.fail_plan = fail_plan

    def emit(self, event: Event) -> None:
        super().emit(event)
        self.seen.append(event)

    def plan_ready(self, info: PlanInfo) -> None:
        if self.fail_plan:
            raise RuntimeError("plan hook failed")
        self.seen.append(info)


def _compiled() -> RunCompiled:
    return RunCompiled(
        units=1, by_materialization={}, ephemeral_inlined=0, selected=1, plan_hash="h", executor="t"
    )


def test_the_bus_calls_the_hook_and_skips_sinks_without_it() -> None:
    plain, hooked = RecordingSink(), PlanSink()
    with EventBus([(plain, 5), (hooked, 5)], clock=CLOCK, invocation_id="i") as bus:
        bus.plan_ready(INFO)
        bus.emit(_compiled())
    assert hooked.seen[0] is INFO
    assert plain.types() == ["run.compiled"]


def test_a_failing_hook_counts_against_the_budget() -> None:
    bad, watcher = PlanSink(fail_plan=True), RecordingSink()
    with EventBus([(bad, 1), (watcher, 5)], clock=CLOCK, invocation_id="i") as bus:
        bus.plan_ready(INFO)
    assert watcher.types() == ["sink.disabled"]


def test_a_buffered_sink_keeps_the_plan_in_order() -> None:
    inner = PlanSink()
    with EventBus([(BufferedSink(inner), 5)], clock=CLOCK, invocation_id="i") as bus:
        bus.emit(_compiled())
        bus.plan_ready(INFO)
        bus.emit(_compiled())
    assert [type(s).__name__ for s in inner.seen] == ["Event", "PlanInfo", "Event"]


def test_a_run_hands_the_manifest_over_right_after_run_started(tmp_path: Path) -> None:
    root = shop(tmp_path)
    sink = PlanSink()
    project = Project(
        ProjectOptions(project_dir=root, target="ci", env={"PIPELOOM_USER": "tester"}),
        clock=CLOCK,
        invocation_id="inv",
    )
    with EventBus([(sink, 5)], clock=CLOCK, invocation_id="inv") as bus:
        assert project.run(RunOptions(Selection(()), run_id="r1", workers=1), bus).exit_code == 0
    kinds = [s.type if isinstance(s, Event) else "plan" for s in sink.seen]
    started = kinds.index("run.started")
    assert kinds[started + 1] == "plan"
    info = sink.seen[started + 1]
    assert isinstance(info, PlanInfo)
    assert info.manifest["metadata"]["project"] == "shop"
    assert "model.shop.fct_orders" in info.manifest["nodes"]
    assert info.project_dir == root.resolve()


def test_model_written_carries_the_written_columns(tmp_path: Path) -> None:
    outcome, sink = run_project(shop(tmp_path))
    assert outcome.exit_code == 0
    written = {e.unit_id: e.data for e in sink.events if e.type == "model.written"}
    assert written["stg_orders"]["columns"] == [
        {"name": "id", "dtype": "int64", "nullable": True},
        {"name": "customer_id", "dtype": "int64", "nullable": True},
        {"name": "amount", "dtype": "float64", "nullable": True},
    ]


def test_columns_default_to_null() -> None:
    payload = ModelWritten(
        relation="r",
        materialization="intermediate",
        strategy=None,
        rows_written=None,
        rows_inserted=None,
        bytes_written=None,
        duration_ms=0,
    )
    assert payload.to_data()["columns"] is None
