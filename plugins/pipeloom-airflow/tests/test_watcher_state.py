"""WATCHER mode, the file side (ADR-0048). No Airflow: these run on every CI cell."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import Report
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import read_manifest, write_manifest
from pipeloom.runtime.retry import UnitPolicy
from pipeloom.runtime.run import RunOptions, _plan_from_manifest, artifacts_root
from pipeloom_airflow.commands import CoreCommand, RunContext
from pipeloom_airflow.config import ExecutionConfig, ExecutionMode, RenderConfig
from pipeloom_airflow.load import LoadedPlan
from pipeloom_airflow.render import render_plan
from pipeloom_airflow.watcher import state
from pipeloom_airflow.watcher.state import (
    Decision,
    UnitView,
    decide,
    in_progress,
    read_unit,
    resolve_artifacts_root,
)

from .conftest import SHOP

POLICY = UnitPolicy(retries=1, retry_delay=0.0, retry_on="transient", timeout=None)


def run_dir(tmp_path: Path) -> Path:
    return tmp_path / "runs" / "r1"


def attempt(
    tmp_path: Path, n: int, status: str, *, category: str | None = None, code: str = "PLM204"
) -> None:
    error = (
        None if status == "succeeded" else {"code": code, "message": "boom", "category": category}
    )
    path = run_dir(tmp_path) / "units" / "u" / f"attempt-{n}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {"unit_id": "u", "status": status, "attempt": n, "retries_used": n - 1, "error": error}
    path.write_text(json.dumps(row), encoding="utf-8")


def results(tmp_path: Path, rows: list[dict[str, Any]]) -> None:
    path = run_dir(tmp_path) / "run_results.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"results": rows}), encoding="utf-8")


def verdict(tmp_path: Path, *, ended: bool = False, policy: UnitPolicy = POLICY) -> Decision:
    return decide(read_unit(run_dir(tmp_path), "u", probe=2), policy, producer_ended=ended)


def test_a_succeeded_attempt_is_success(tmp_path: Path) -> None:
    attempt(tmp_path, 1, "failed", category="transient")
    attempt(tmp_path, 2, "succeeded")
    d = verdict(tmp_path)
    assert d.kind == "success" and d.attempt is not None and d.attempt.number == 2


def test_a_transient_failure_with_retries_left_waits(tmp_path: Path) -> None:
    attempt(tmp_path, 1, "failed", category="transient")
    assert verdict(tmp_path).kind == "wait"


def test_a_permanent_failure_under_retry_on_transient_fails(tmp_path: Path) -> None:
    attempt(tmp_path, 1, "failed", category="permanent", code="PLQ301")
    d = verdict(tmp_path)
    assert (d.kind, d.code, d.message) == ("failed", "PLQ301", "boom")


def test_retries_used_up_fails(tmp_path: Path) -> None:
    attempt(tmp_path, 1, "failed", category="transient")
    attempt(tmp_path, 2, "failed", category="transient")
    assert verdict(tmp_path).kind == "failed"


@pytest.mark.parametrize("status", ["timed_out", "cancelled"])
def test_timed_out_and_cancelled_attempts_fail(tmp_path: Path, status: str) -> None:
    attempt(tmp_path, 1, status, category="transient", code="PLX103")
    assert verdict(tmp_path).kind == "failed"


def test_a_skipped_row_runs_here(tmp_path: Path) -> None:
    results(tmp_path, [{"unit_id": "u", "status": "skipped"}])
    assert verdict(tmp_path, ended=True).kind == "run"


def test_a_cancelled_row_fails(tmp_path: Path) -> None:
    results(tmp_path, [{"unit_id": "u", "status": "cancelled", "error": None}])
    d = verdict(tmp_path)
    assert d.kind == "failed" and d.message is not None


def test_a_terminal_row_without_its_file_is_invisible(tmp_path: Path) -> None:
    attempt(tmp_path, 1, "failed", category="transient")
    results(tmp_path, [{"unit_id": "u", "status": "succeeded", "attempt": 2}])
    d = verdict(tmp_path)
    assert (d.kind, d.code) == ("invisible", "PLA004")


@pytest.mark.parametrize("status", ["failed", "timed_out", "cancelled"])
def test_a_terminal_row_for_the_visible_attempt_uses_its_status(
    tmp_path: Path, status: str
) -> None:
    # retries_used says one retry is left, but the row says attempt 1 was the last
    attempt(tmp_path, 1, "failed", category="transient")
    results(tmp_path, [{"unit_id": "u", "status": status, "attempt": 1}])
    d = verdict(tmp_path)
    assert (d.kind, d.code, d.message) == ("failed", "PLM204", "boom")


def test_no_row_for_the_unit_is_invisible(tmp_path: Path) -> None:
    results(tmp_path, [{"unit_id": "other", "status": "succeeded"}])
    assert verdict(tmp_path).kind == "invisible"


def test_no_results_once_the_producer_ended(tmp_path: Path) -> None:
    assert verdict(tmp_path).kind == "wait"
    d = verdict(tmp_path, ended=True)
    assert (d.kind, d.code) == ("no_results", "PLA009")


def test_in_progress() -> None:
    empty = UnitView((), None, False)
    assert in_progress(empty, POLICY, producer_active=True)
    assert not in_progress(empty, POLICY, producer_active=False)


def test_a_final_verdict_is_not_in_progress(tmp_path: Path) -> None:
    attempt(tmp_path, 1, "succeeded")
    view = read_unit(run_dir(tmp_path), "u", probe=1)
    assert not in_progress(view, POLICY, producer_active=True)


def test_a_stale_listing_still_finds_the_file_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    attempt(tmp_path, 1, "succeeded")

    def hidden(self: Path) -> Iterator[Path]:
        return iter(())

    monkeypatch.setattr(Path, "iterdir", hidden)
    assert [a.number for a in read_unit(run_dir(tmp_path), "u", probe=2).attempts] == [1]
    assert read_unit(run_dir(tmp_path), "u", probe=0).attempts == ()


def test_the_listing_finds_attempts_above_the_probe(tmp_path: Path) -> None:
    attempt(tmp_path, 3, "succeeded")
    assert [a.number for a in read_unit(run_dir(tmp_path), "u", probe=1).attempts] == [3]


def test_partial_json_is_not_visible(tmp_path: Path) -> None:
    attempt(tmp_path, 1, "succeeded")
    (run_dir(tmp_path) / "units" / "u" / "attempt-2.json").write_text('{"status": "su', "utf-8")
    (run_dir(tmp_path) / "run_results.json").write_text('{"results": [', encoding="utf-8")
    view = read_unit(run_dir(tmp_path), "u", probe=2)
    assert [a.number for a in view.attempts] == [1] and not view.results_present


def test_a_missing_run_folder_reads_empty(tmp_path: Path) -> None:
    assert read_unit(run_dir(tmp_path), "u", probe=3) == UnitView((), None, False)


def test_constants() -> None:
    assert (state.RECHECK_S, state.VISIBILITY_GRACE_S) == (300.0, 120.0)
    assert "running" not in state.TERMINAL_TASK_STATES
    assert "upstream_failed" in state.TERMINAL_TASK_STATES


HEAD = "config_version: 1\nname: p\nengine:\n  kind: memory\ncatalog:\n  kind: memory\n"
LAYOUTS = {
    "default": "targets:\n  ci: {}\n",
    "top": "artifacts:\n  root: out/a\ntargets:\n  ci: {}\n",
    "target": "artifacts:\n  root: out/a\ntargets:\n  ci:\n    artifacts:\n      root: out/b\n",
    "whole": "artifacts:\n  root: out/a\ntargets:\n  ci:\n    artifacts:\n      intermediate_ttl: 0\n",
    "absolute": "artifacts:\n  root: ABS\ntargets:\n  ci: {}\n",
}


@pytest.mark.parametrize("layout", sorted(LAYOUTS))
def test_the_root_matches_the_core(tmp_path: Path, layout: str) -> None:
    absolute = (tmp_path / "abs").as_posix()
    (tmp_path / "pipeloom.yml").write_text(
        HEAD + LAYOUTS[layout].replace("ABS", absolute), encoding="utf-8"
    )
    bound = Project(ProjectOptions(project_dir=tmp_path, target="ci", env={})).bind()
    assert bound.value is not None, [d.message for d in bound.diagnostics]
    exec_dir = tmp_path / "exec"
    report = Report()
    got = resolve_artifacts_root(tmp_path.as_posix(), exec_dir.as_posix(), "ci", None, report)
    assert report.sorted() == () and got is not None
    assert Path(got) == artifacts_root(bound.value, exec_dir)


def test_a_relative_root_joins_a_windows_or_posix_exec_dir(tmp_path: Path) -> None:
    (tmp_path / "pipeloom.yml").write_text(HEAD + LAYOUTS["top"], encoding="utf-8")
    p = tmp_path.as_posix()
    assert resolve_artifacts_root(p, "/srv/p", "ci", None, Report()) == "/srv/p/out/a"
    assert resolve_artifacts_root(p, "C:\\p", "ci", None, Report()) == "C:\\p\\out/a"


def test_the_override_wins_without_reading(tmp_path: Path) -> None:
    report = Report()
    assert resolve_artifacts_root("/nowhere", "/x", "ci", "/root", report) == "/root"
    assert report.sorted() == ()


def test_an_unreadable_file_is_pla008(tmp_path: Path) -> None:
    report = Report()
    assert resolve_artifacts_root(tmp_path.as_posix(), "/x", "ci", None, report) is None
    assert [d.code for d in report.sorted()] == ["PLA008"]


def test_an_interpolated_root_is_pla008(tmp_path: Path) -> None:
    (tmp_path / "pipeloom.yml").write_text(
        HEAD + "artifacts:\n  root: ${env:ROOT}\ntargets:\n  ci: {}\n", encoding="utf-8"
    )
    report = Report()
    assert resolve_artifacts_root(tmp_path.as_posix(), "/x", "ci", None, report) is None
    assert [d.code for d in report.sorted()] == ["PLA008"]


def units_run(manifest: Path, render: RenderConfig) -> list[str]:
    """The units `pipeloom run --manifest` runs for the producer's command line."""
    ctx = RunContext("r", datetime(2026, 1, 1, tzinfo=UTC), False, {})
    argv = CoreCommand("/p", "ci", None, "h", None).producer_argv(
        ["py"], ctx, select=render.select, exclude=render.exclude, workers=None
    )

    def values(flag: str) -> tuple[str, ...]:
        return tuple(argv[i + 1] for i, a in enumerate(argv) if a == flag)

    project = Project(ProjectOptions(project_dir=SHOP, target="ci", env={}))
    opts = RunOptions(
        selection=Selection(values("--select"), values("--exclude")), manifest=manifest
    )
    plan, _ = _plan_from_manifest(project, opts, Report())
    assert plan is not None
    chosen = set(plan.selection.selected)
    return sorted(u for u, unit in plan.units.items() if unit.nodes[-1] in chosen)


@pytest.mark.parametrize("stable", [True, False])
@pytest.mark.parametrize(
    ("select", "exclude"), [((), ()), (("stg_orders+",), ()), ((), ("stg_customers",))]
)
def test_the_producer_runs_the_rendered_units(
    tmp_path: Path, stable: bool, select: tuple[str, ...], exclude: tuple[str, ...]
) -> None:
    compiled = Project(ProjectOptions(project_dir=SHOP, target="ci", env={})).compile(
        Selection(("stg_orders",))  # a manifest that records a narrower selection
    )
    assert compiled.value is not None
    manifest = tmp_path / "manifest.json"
    write_manifest(compiled.value, manifest, stable=stable)
    plan = read_manifest(manifest, Report())
    assert plan is not None
    render = RenderConfig(select=select, exclude=exclude)
    rendered = render_plan(
        LoadedPlan(plan, plan.metadata.plan_hash, None, "manifest"),
        "ci",
        render,
        ExecutionConfig(mode=ExecutionMode.WATCHER),
        Report(),
    )
    want = sorted(u for t in rendered.tasks for u in t.units)
    assert want and units_run(manifest, render) == want
