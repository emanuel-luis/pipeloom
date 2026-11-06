"""`run --state ... --defer`: unselected parents are read from the reference's relations."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli import main
from pipeloom.events.bus import EventBus
from pipeloom.graph.selectors import Selection
from pipeloom.plan.defer import deferred_relations
from pipeloom.plan.manifest import write_manifest
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.recording import RecordingSink
from tests.conftest import make_project
from tests.runtime.test_runner import engine_block, normalize, results

ENV = {"PIPELOOM_USER": "tester"}
SEEDED = {"main.parent": [{"x": 5}], "main.mid": [{"z": 9}]}


def build(tmp_path: Path, *, seed: bool = True) -> Path:
    """Targets prod (relations `main.<name>`) and dev (`dev_main.<name>`) share one store."""
    make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: dfr\n"
                + engine_block(tmp_path, SEEDED if seed else {})
                + "\ncatalog: {kind: memory}\n"
                "targets:\n"
                "  prod:\n    naming: {kind: none}\n"
                "  dev:\n    naming: {kind: prefix, schema_prefix: dev_}\n"
            ),
            "models/m.yml": (
                "models:\n"
                "  - {name: parent, sql: select 1 as x}\n"
                "  - {name: child, sql: select x + 1 as y from parent, "
                "inputs: {parent: {ref: parent}}}\n"
                "  - {name: mid, materialized: intermediate, sql: select 2 as z}\n"
                "  - {name: top, sql: select z from mid, inputs: {mid: {ref: mid}}}\n"
            ),
        },
    )
    return tmp_path


def reference(root: Path) -> None:
    """Compile prod with --stable into state/, the reference a dev run defers to."""
    options = ProjectOptions(project_dir=root, target="prod", env=ENV)
    plan = Project(options).compile().value
    assert plan is not None
    write_manifest(plan, root / "state" / "manifest.json", stable=True)


def go(
    root: Path, *select: str, defer: bool = True, state: bool = True, **opts: Any
) -> tuple[RunOutcome, RecordingSink]:
    clock = FakeClock()
    project = Project(
        ProjectOptions(project_dir=root, target="dev", env=ENV),
        clock=clock,
        invocation_id="inv",
    )
    sink = RecordingSink()
    run_opts = RunOptions(
        Selection(select),
        run_id=opts.pop("run_id", "r1"),
        workers=1,
        state=Path("state") if state else None,
        defer=defer and state,
        **opts,
    )
    with EventBus([(sink, 5)], clock=clock, invocation_id="inv") as bus:
        return project.run(run_opts, bus), sink


def reads(sink: RecordingSink) -> list[dict[str, Any]]:
    return [dict(e.data) for e in sink.events if e.type == "model.input_read"]


def codes(outcome: RunOutcome) -> set[str]:
    return {d.code for d in outcome.diagnostics}


def test_defer_reads_an_unselected_parent_from_the_reference(
    tmp_path: Path, golden: Callable[[str, str], None]
) -> None:
    root = build(tmp_path)
    reference(root)
    outcome, sink = go(root, "child")
    assert outcome.exit_code == 0, outcome.diagnostics
    store = json.loads((root / "store" / "dev_main.child.json").read_text(encoding="utf-8"))
    assert store == [{"y": 6}]  # parent's rows come from the seeded main.parent, not dev_main
    assert not (root / "store" / "dev_main.parent.json").exists()
    (read,) = reads(sink)
    assert read["deferred"] is True and read["relation"] == "main.parent"
    doc = results(root, "r1")
    assert doc["args"]["state"] == "state" and doc["args"]["defer"] is True
    text = json.dumps(normalize(doc), indent=2, ensure_ascii=False) + "\n"
    golden("run_results/defer.json", text.replace(root.as_posix(), "<project>"))


def test_without_defer_the_parent_is_read_from_this_target(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    outcome, _ = go(root, "child", defer=False)
    assert outcome.exit_code == 1 and "PLM204" in codes(outcome)
    assert results(root, "r1")["args"]["defer"] is False


def test_a_missing_reference_table_is_plm204_with_the_defer_hint(tmp_path: Path) -> None:
    root = build(tmp_path, seed=False)
    reference(root)
    outcome, _ = go(root, "child")
    (failed,) = [d for d in outcome.diagnostics if d.code == "PLM204"]
    assert failed.hint == (
        "--defer reads model 'parent' from the reference relation 'main.parent'; check that it "
        "exists and that this target can read it, or select the model to build it here."
    )


def test_defer_never_changes_the_plan_hash(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    go(root, "child", run_id="with")
    go(root, "child", defer=False, run_id="without")
    hashes = {results(root, r)["run"]["plan_hash"] for r in ("with", "without")}
    assert len(hashes) == 1


def test_state_selection_and_defer_together(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    (root / "models" / "m.yml").write_text(
        (root / "models" / "m.yml").read_text(encoding="utf-8").replace("x + 1", "x + 2"),
        encoding="utf-8",
        newline="\n",
    )
    outcome, sink = go(root, "state:modified")
    assert outcome.exit_code == 0, outcome.diagnostics
    assert [r["unit_id"] for r in results(root, "r1")["results"]] == ["child"]
    store = json.loads((root / "store" / "dev_main.child.json").read_text(encoding="utf-8"))
    assert store == [{"y": 7}]
    assert [r["relation"] for r in reads(sink)] == ["main.parent"]


def test_an_intermediate_parent_is_read_here_with_plx112(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    outcome, _ = go(root, "top")
    assert "PLX112" in codes(outcome)
    warned, _ = go(root, "top", warn_error=True, run_id="r2")
    assert warned.exit_code == 1 and not (root / "target" / "runs" / "r2").exists()


def test_a_parent_missing_from_the_reference_is_plx112(tmp_path: Path) -> None:
    root = build(tmp_path)
    options = ProjectOptions(project_dir=root, target="dev", env=ENV)
    plan = Project(options).compile().value
    assert plan is not None
    parent = "model.dfr.parent"
    older = plan.model_copy(update={"nodes": {k: v for k, v in plan.nodes.items() if k != parent}})
    deferred, found = deferred_relations(plan, ["child"], older)
    assert parent not in deferred and [d.code for d in found] == ["PLX112"]
    deferred, found = deferred_relations(plan, ["child"], plan)
    assert deferred[parent].relation_name == "dev_main.parent" and found == []


def test_defer_is_refused_for_a_unit_invocation(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    outcome, _ = go(root, "unit:child")
    assert outcome.exit_code == 2 and codes(outcome) == {"PLX111"}


def test_defer_is_refused_for_the_task_boundary(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    outcome, _ = go(root, "child", boundary="task")
    assert outcome.exit_code == 2 and codes(outcome) == {"PLX111"}


def test_defer_is_refused_for_another_catalog_kind(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    manifest = root / "state" / "manifest.json"
    doc = json.loads(manifest.read_text(encoding="utf-8"))
    doc["metadata"]["plugins"]["catalogs"]["kind"] = "other"
    manifest.write_text(json.dumps(doc), encoding="utf-8")
    outcome, _ = go(root, "child")
    assert outcome.exit_code == 2 and codes(outcome) == {"PLX111"}


def test_defer_needs_state() -> None:
    with pytest.raises(ValueError, match="defer needs state"):
        RunOptions(defer=True)


def cli_run(root: Path, *args: str) -> Any:
    base = ["run", "--project-dir", str(root), "--target", "dev"]
    return CliRunner().invoke(main, [*base, *args], env=ENV)


def test_cli_defer_without_state_is_a_usage_error(tmp_path: Path) -> None:
    result = cli_run(build(tmp_path), "--defer", "-s", "child")
    assert result.exit_code == 2
    assert "--defer needs --state" in result.stderr


def test_cli_defer_refused_exits_2(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    result = cli_run(root, "--state", "state", "--defer", "-s", "unit:child")
    assert result.exit_code == 2
    assert "PLX111" in result.stderr


def test_manifest_runs_may_defer(tmp_path: Path) -> None:
    root = build(tmp_path)
    reference(root)
    plan = Project(ProjectOptions(project_dir=root, target="dev", env=ENV)).compile().value
    assert plan is not None
    write_manifest(plan, root / "dev.json", stable=True)
    outcome, sink = go(root, "child", manifest=root / "dev.json")
    assert outcome.exit_code == 0, outcome.diagnostics
    assert reads(sink)[0]["deferred"] is True
