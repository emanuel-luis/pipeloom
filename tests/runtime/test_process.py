"""The process executor and unit mode, on the memory engine with JSON tables and the json state."""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import write_manifest
from pipeloom.runtime.executors.process import ChildCommand
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import JsonTableStore
from pipeloom.testing.recording import RecordingSink
from tests.conftest import make_project

PIPELOOM_YML = """config_version: 1
name: p
python_paths: [src]
boundary: task
targets:
  ci:
    engine: {kind: memory, root: store}
    catalog: {kind: memory, root: store}
    state: {kind: json, path: state}
    naming: {kind: none}
"""
CHAIN = """models:
  - {name: a, sql: select 1 as x}
  - {name: b, sql: select * from a, inputs: {a: {ref: a}}}
  - {name: c, sql: select * from b, inputs: {b: {ref: b}}}
"""
CRASH = """models:
  - {name: a, sql: select 1 as x}
  - {name: boom, python: "m:die", inputs: {a: {ref: a}}}
  - {name: after, sql: select * from boom, inputs: {boom: {ref: boom}}}
"""
DIE = "def die(ctx, a):\n    import os\n\n    os._exit(3)\n"


def project_dir(tmp_path: Path, models: str = CHAIN, src: str | None = None) -> Path:
    files: dict[str, Any] = {"pipeloom.yml": PIPELOOM_YML, "models/m.yml": models}
    if src is not None:
        files["src/m.py"] = src
    return make_project(tmp_path, files)


def run(
    root: Path, opts: RunOptions, *, invocation_id: str = "parent"
) -> tuple[RunOutcome, RecordingSink]:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    project = Project(
        ProjectOptions(project_dir=root, target="ci", env={"PIPELOOM_USER": "tester"}),
        clock=clock,
        invocation_id=invocation_id,
    )
    sink = RecordingSink()
    with EventBus([(sink, 5)], clock=clock, invocation_id=invocation_id) as bus:
        return project.run(opts, bus), sink


def results(root: Path, run_id: str) -> dict[str, Any]:
    path = root / "target" / "runs" / run_id / "run_results.json"
    doc: dict[str, Any] = json.loads(path.read_bytes().decode("utf-8"))
    return doc


def test_child_command_argv(tmp_path: Path) -> None:
    cmd = ChildCommand(
        project_dir=tmp_path,
        target="ci",
        manifest=tmp_path / "m.json",
        run_id="r",
        plan_hash="0" * 64,
        full_refresh=True,
        vars='{"k":1}',
        plugins=("a:B",),
    )
    argv = cmd.argv("u")
    assert argv[1:5] == ["-m", "pipeloom", "run", "--log-format"]
    assert argv[argv.index("--select") + 1] == "unit:u"
    assert argv[argv.index("--full-refresh") + 1] == "true"
    assert argv[argv.index("--vars") + 1] == '{"k":1}'
    assert argv[argv.index("--plugin") + 1] == "a:B"
    assert argv[argv.index("--expect-plan-hash") + 1] == "0" * 64


def test_every_unit_runs_in_its_own_process(tmp_path: Path) -> None:
    root = project_dir(tmp_path)
    outcome, sink = run(root, RunOptions(run_id="r1", workers=2))
    assert (outcome.status, outcome.exit_code) == ("succeeded", 0)
    assert JsonTableStore(root / "store")["main.c"] == ({"x": 1},)
    doc = results(root, "r1")
    assert [(r["unit_id"], r["status"], r["executor"]) for r in doc["results"]] == [
        ("a", "succeeded", "process"),
        ("b", "succeeded", "process"),
        ("c", "succeeded", "process"),
    ]
    assert doc["args"]["boundary"] == "task"
    started = [e for e in sink.events if e.type == "run.started"]
    assert started[0].data["executor"] == "process" and started[0].data["boundary"] == "task"
    # Unit events come from the children: their invocation id and sequence numbers are kept.
    unit = [e for e in sink.events if e.type in ("unit.started", "unit.succeeded")]
    assert {e.invocation_id for e in unit}.isdisjoint({"parent"})
    for inv in {e.invocation_id for e in unit}:
        seqs = [e.seq for e in unit if e.invocation_id == inv]
        assert seqs == sorted(seqs)
    assert Counter(e.unit_id for e in sink.events if e.type == "unit.succeeded") == {
        "a": 1,
        "b": 1,
        "c": 1,
    }
    # The parent emits the scheduler events and no invocation events of the children.
    assert Counter(e.invocation_id for e in sink.events if e.type == "unit.queued") == {"parent": 3}
    assert not [e for e in sink.events if e.type.startswith("invocation.")]


def test_children_write_their_own_event_files_and_logs(tmp_path: Path) -> None:
    root = project_dir(tmp_path)
    run(root, RunOptions(run_id="r1", workers=1))
    events = sorted((root / "target" / "runs" / "r1" / "events").glob("*.jsonl"))
    assert len(events) == 3  # one per child; the parent wrote none here, it has no run file sink
    assert (root / "target" / "runs" / "r1" / "logs").is_dir()


def test_a_child_that_dies_without_a_result_is_plx106(tmp_path: Path) -> None:
    root = project_dir(tmp_path, CRASH, DIE)
    outcome, sink = run(root, RunOptions(run_id="r1", workers=1))
    assert outcome.exit_code == 1
    plx106 = [d for d in outcome.diagnostics if d.code == "PLX106"]
    assert len(plx106) == 1 and "exited with code 3" in plx106[0].message
    assert plx106[0].context["unit"] == "boom"
    doc = results(root, "r1")
    states = {r["unit_id"]: r["status"] for r in doc["results"]}
    assert states == {"a": "succeeded", "after": "skipped", "boom": "failed"}
    failed = [e for e in sink.events if e.type == "unit.failed"]
    assert [e.unit_id for e in failed] == ["boom"]  # one terminal event, not two
    done = next(e for e in sink.events if e.type == "run.completed")
    assert done.data["failures"][0]["code"] == "PLX106"


def test_a_held_lock_is_plx105_unless_no_lock(tmp_path: Path) -> None:
    root = project_dir(tmp_path)
    bound = Project(ProjectOptions(project_dir=root, target="ci", env={})).bind().value
    assert bound is not None
    store = bound.plugins["state_stores"].create()
    with store.lock("p/ci", "other"):
        outcome, sink = run(root, RunOptions(run_id="r1"))
        assert outcome.exit_code == 1
        assert [d.code for d in outcome.diagnostics] == ["PLX105"]
        assert "other" in outcome.diagnostics[0].message
        assert not [e for e in sink.events if e.type == "run.started"]
        free, _ = run(root, RunOptions(run_id="r2", lock=False))
        assert free.exit_code == 0
    store.close()


def test_unit_mode_has_no_run_events_lock_or_results(tmp_path: Path) -> None:
    root = project_dir(tmp_path)
    outcome, sink = run(root, RunOptions(Selection(("unit:a",)), run_id="u1"))
    assert (outcome.status, outcome.exit_code, outcome.run_results) == ("succeeded", 0, None)
    types = {e.type for e in sink.events}
    assert "unit.succeeded" in types and "unit.started" in types
    assert not {t for t in types if t.startswith(("run.", "unit.queued", "artifact."))}
    run_dir = root / "target" / "runs" / "u1"
    assert (run_dir / "units" / "a" / "attempt-1.json").is_file()
    assert not (run_dir / "run_results.json").exists() and not (run_dir / "manifest.json").exists()
    assert not (root / "target" / "run_results.json").exists()


def test_unit_mode_runs_on_threads_even_when_the_boundary_is_task(tmp_path: Path) -> None:
    root = project_dir(tmp_path)
    run(root, RunOptions(Selection(("unit:a",)), run_id="u1"))
    attempt = json.loads((root / "target/runs/u1/units/a/attempt-1.json").read_bytes())
    assert attempt["executor"] == "process"  # the name of the owner's executor, run in-process


def test_unknown_unit_is_plg306(tmp_path: Path) -> None:
    root = project_dir(tmp_path)
    outcome, _ = run(root, RunOptions(Selection(("unit:nope",))))
    assert [d.code for d in outcome.diagnostics] == ["PLG306"]
    assert outcome.exit_code == 1


def manifest_of(root: Path) -> tuple[Path, str]:
    project = Project(ProjectOptions(project_dir=root, target="ci", env={}))
    plan = project.compile().value
    assert plan is not None
    path = root / "target" / "m.json"
    write_manifest(plan, path)
    return path, plan.metadata.plan_hash


def test_manifest_and_expected_hash(tmp_path: Path) -> None:
    root = project_dir(tmp_path)
    path, digest = manifest_of(root)
    outcome, _ = run(
        root,
        RunOptions(manifest=path, expect_plan_hash=digest, run_id="m1", workers=1),
    )
    assert outcome.exit_code == 0
    assert results(root, "m1")["args"]["manifest"] == "target/m.json"


def test_wrong_plan_hash_is_plx103(tmp_path: Path) -> None:
    root = project_dir(tmp_path)
    path, _ = manifest_of(root)
    for opts in (
        RunOptions(manifest=path, expect_plan_hash="0" * 64),
        RunOptions(expect_plan_hash="0" * 64),
    ):
        outcome, sink = run(root, opts)
        assert [d.code for d in outcome.diagnostics] == ["PLX103"]
        assert outcome.exit_code == 1 and not [e for e in sink.events if e.type == "run.started"]


@pytest.mark.parametrize("edit", ["not json", "{}", "other project", "edited"])
def test_a_bad_manifest_is_plx107(tmp_path: Path, edit: str) -> None:
    root = project_dir(tmp_path)
    path, _ = manifest_of(root)
    text = path.read_text(encoding="utf-8")
    if edit == "not json":
        text = "{"
    elif edit == "{}":
        text = "{}"
    elif edit == "other project":
        data = json.loads(text)
        data["metadata"]["project"] = "other"
        text = json.dumps(data)
    else:
        text = text.replace('"select 1 as x"', '"select 2 as x"')
        assert text != path.read_text(encoding="utf-8")
    path.write_text(text, encoding="utf-8", newline="\n")
    outcome, _ = run(root, RunOptions(manifest=path))
    assert [d.code for d in outcome.diagnostics] == ["PLX107"]
    assert outcome.diagnostics[0].location is not None
    assert outcome.diagnostics[0].location.file == "target/m.json"
    assert outcome.exit_code == 1


def test_a_child_that_stops_before_its_unit_leaves_its_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = ChildCommand.argv

    def wrong_hash(self: ChildCommand, unit_id: str) -> list[str]:
        argv = real(self, unit_id)
        argv[argv.index("--expect-plan-hash") + 1] = "0" * 64
        return argv

    monkeypatch.setattr(ChildCommand, "argv", wrong_hash)
    root = project_dir(tmp_path)
    outcome, _ = run(root, RunOptions(run_id="r1", workers=1))
    (plx106,) = [d for d in outcome.diagnostics if d.code == "PLX106"][:1]
    log = (root / "target/runs/r1/logs/a-attempt-1.log").read_text(encoding="utf-8")
    assert "PLX103" in log
    row = json.loads((root / "target/runs/r1/units/a/attempt-1.json").read_bytes())
    assert any("PLX103" in line for line in row["error"]["trace_excerpt"])
    assert plx106.context["unit"] == "a"
    # the child had no run id yet, so its events went to the fallback file
    assert list((root / "target/runs/_invocations").glob("*.jsonl"))
