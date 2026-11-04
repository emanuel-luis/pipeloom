"""How a run reads on the console, the relayed events of a child, and the run file sink."""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from click.testing import CliRunner
from rich.console import Console

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli import main
from pipeloom.events.bus import EventBus
from pipeloom.events.model import UnitQueued
from pipeloom.events.sinks.console import ConsoleSink
from pipeloom.events.sinks.runfile import RunFileSink
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink
from tests.conftest import make_project

PROJECT = {
    "pipeloom.yml": (
        "config_version: 1\nname: shop\npython_paths: [src]\n"
        "targets:\n  dev:\n    engine: {kind: memory}\n    catalog: {kind: memory}\n"
    ),
    "models/m.yml": (
        "models:\n"
        "  - {name: stg_customers, sql: 'select 1 as id union all select 2'}\n"
        "  - {name: fct_payments, python: 'm:build', inputs: {c: {ref: stg_customers}}}\n"
        "  - {name: rpt_revenue, sql: select * from p, inputs: {p: {ref: fct_payments}}}\n"
        "  - {name: dim_view, sql: select * from c, inputs: {c: {ref: stg_customers}},"
        " materialized: view}\n"
    ),
    "src/m.py": "def build(ctx, c):\n    raise KeyError('amount')\n",
}


def clock() -> FakeClock:
    return FakeClock(datetime(2026, 1, 1, 14, 2, 11, tzinfo=UTC), step=timedelta(0))


def test_run_console_golden(tmp_path: Path, golden: Callable[[str, str], None]) -> None:
    root = make_project(tmp_path, PROJECT)
    out = io.StringIO()
    console = Console(file=out, width=100, color_system=None, force_terminal=False)
    fake = clock()
    project = Project(
        ProjectOptions(project_dir=root, env={"PIPELOOM_USER": "tester"}),
        clock=fake,
        invocation_id="inv",
    )
    with EventBus([(ConsoleSink(console, tz=UTC), 5)], clock=fake, invocation_id="inv") as bus:
        outcome = project.run(RunOptions(run_id="20261029T140211Z-7f3a9c", workers=1), bus)
    assert outcome.exit_code == 1
    golden("events/console_run.txt", out.getvalue())


def test_forward_keeps_the_envelope_and_skips_run_file_sinks(tmp_path: Path) -> None:
    seen = RecordingSink()
    file = RunFileSink(lambda rid: tmp_path / rid / "e.jsonl")
    other = EventBus([], clock=clock(), invocation_id="child")
    with EventBus([(seen, 5), (file, 5)], clock=clock(), invocation_id="parent") as bus:
        bus.set_run_id("r")
        own = bus.emit(UnitQueued(downstream=1), unit_id="a")
        child = other.emit(UnitQueued(downstream=0), unit_id="b")
        bus.forward(child)
    assert [(e.invocation_id, e.seq, e.unit_id) for e in seen.events] == [
        ("parent", 1, "a"),
        ("child", 1, "b"),
    ]
    assert own.invocation_id == "parent"
    lines = (tmp_path / "r" / "e.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(ln)["unit_id"] for ln in lines] == ["a"]  # the relayed event is not in it


def test_run_file_sink_holds_events_until_the_run_id_is_known(tmp_path: Path) -> None:
    path = tmp_path / "runs" / "r1" / "events" / "i.jsonl"
    sink = RunFileSink(lambda rid: path.parent.parent.parent / rid / "events" / "i.jsonl")
    with EventBus([(sink, 5)], clock=clock(), invocation_id="i") as bus:
        bus.emit(UnitQueued(downstream=0), unit_id="early")
        assert not path.exists()
        bus.set_run_id("r1")
        bus.emit(UnitQueued(downstream=0), unit_id="late")
    ids = [json.loads(ln)["unit_id"] for ln in path.read_text(encoding="utf-8").splitlines()]
    assert ids == ["early", "late"]


def test_a_task_run_prints_the_units_the_children_finished(tmp_path: Path) -> None:
    root = make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: p\nboundary: task\ntargets:\n  ci:\n"
                "    engine: {kind: memory, root: store}\n"
                "    catalog: {kind: memory, root: store}\n"
                "    state: {kind: json, path: state}\n"
            ),
            "models/m.yml": "models:\n  - {name: a, sql: select 1 as x}\n",
        },
    )
    argv = ["run", "--project-dir", str(root), "--target", "ci", "--run-id", "r1"]
    r = CliRunner().invoke(main, argv, env={"PIPELOOM_TARGET": "", "PIPELOOM_LOG_FORMAT": ""})
    assert r.exit_code == 0, r.stderr
    assert "executor process" in r.stderr and "1/1 OK" in r.stderr
    assert r.stderr.count("1/1 OK") == 1
