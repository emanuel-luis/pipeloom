"""The model runner and `Project.run` on the memory engine."""

from __future__ import annotations

import json
import shutil
import sys
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.graph.selectors import Selection
from pipeloom.runtime.run import RunOptions, RunOutcome
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import JsonTableStore
from pipeloom.testing.recording import RecordingSink
from tests.conftest import FIXTURES, make_project

Rows = list[dict[str, Any]]


def tables_for(root: Path) -> dict[str, Rows]:
    """Source tables, keyed by the absolute location the runner gives the engine."""
    key = f"{root.as_posix()}/raw"
    return {
        f"{key}/customers": [{"id": 1, "email": "A@X.COM"}, {"id": 2, "email": "B@X.COM"}],
        f"{key}/orders": [
            {"id": 10, "customer_id": 1, "amount": 5.0},
            {"id": 11, "customer_id": 2, "amount": 7.5},
            {"id": 12, "customer_id": 1, "amount": 2.5},
        ],
        f"{key}/payments": [{"order_id": 10, "amount": 5.0}],
    }


def engine_block(root: Path, tables: Mapping[str, Rows]) -> str:
    """The engine's tables live as JSON files under `root/store`, so a test can read results."""
    cfg = {"kind": "memory", "root": (root / "store").as_posix(), "tables": dict(tables)}
    return f"engine: {json.dumps(cfg)}"


def shop(tmp_path: Path) -> Path:
    shutil.copytree(
        FIXTURES / "shop", tmp_path, ignore=shutil.ignore_patterns("target"), dirs_exist_ok=True
    )
    yml = tmp_path / "pipeloom.yml"
    text = yml.read_text(encoding="utf-8").replace(
        "engine: {kind: memory}", engine_block(tmp_path, tables_for(tmp_path))
    )
    yml.write_text(text, encoding="utf-8", newline="\n")
    return tmp_path


def run_project(
    project_dir: Path,
    *,
    select: tuple[str, ...] = (),
    run_id: str = "run1",
    full_refresh: bool = False,
    fail_fast: bool = False,
    workers: int = 1,
    invocation_id: str = "inv",
    hour: int = 12,
) -> tuple[RunOutcome, RecordingSink]:
    clock = FakeClock(datetime(2026, 1, 1, hour, tzinfo=UTC), step=timedelta(0))
    project = Project(
        ProjectOptions(project_dir=project_dir, target="ci", env={"PIPELOOM_USER": "tester"}),
        clock=clock,
        invocation_id=invocation_id,
    )
    sink = RecordingSink()
    with EventBus([(sink, 5)], clock=clock, invocation_id=invocation_id) as bus:
        outcome = project.run(
            RunOptions(
                Selection(select),
                run_id=run_id,
                full_refresh=full_refresh,
                fail_fast=fail_fast,
                workers=workers,
            ),
            bus,
        )
    return outcome, sink


def store(project_dir: Path) -> JsonTableStore:
    return JsonTableStore(project_dir / "store")


def results(project_dir: Path, run_id: str = "run1") -> dict[str, Any]:
    path = project_dir / "target" / "runs" / run_id / "run_results.json"
    doc: dict[str, Any] = json.loads(path.read_bytes().decode("utf-8"))
    return doc


def normalize(doc: dict[str, Any]) -> dict[str, Any]:
    """Blank what changes between machines and runs: times, host, user, versions, git."""

    def walk(value: Any, key: str = "") -> Any:
        if isinstance(value, dict):
            return {k: walk(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [walk(v, key) for v in value]
        if key.endswith("_at") and isinstance(value, str):
            return "<timestamp>"
        if key in ("host", "python_version", "git_sha", "pipeloom_version", "version"):
            return f"<{key}>"
        if key == "user":
            return "<user>"
        return value

    out: dict[str, Any] = walk(doc)
    return out


def test_shop_runs_end_to_end(tmp_path: Path, golden: Callable[[str, str], None]) -> None:
    root = shop(tmp_path)
    outcome, sink = run_project(root)
    assert (outcome.status, outcome.exit_code) == ("succeeded", 0)
    assert outcome.run_results == root / "target" / "runs" / "run1" / "run_results.json"
    data = store(root)
    assert [r["email"] for r in data["main.stg_customers"]] == ["a@x.com", "b@x.com"]
    assert len(data["main.fct_orders"]) == 3 and len(data["main.dim_customers"]) == 3
    assert {r["customer_id"]: r["revenue"] for r in data["main.rpt_revenue"]} == {1: 7.5, 2: 7.5}
    sink.assert_sequence(
        "run.started", "run.compiled", "unit.started", "unit.succeeded", "run.completed"
    )
    assert sink.types().count("unit.succeeded") == 6
    assert sink.types().count("artifact.written") == 4
    doc = results(root)
    assert [r["unit_id"] for r in doc["results"]] == sorted(r["unit_id"] for r in doc["results"])
    assert doc["run"]["counts"]["succeeded"] == 6 and doc["run"]["exit_code"] == 0
    for name in ("manifest.json", "run_results.json"):
        assert (root / "target" / name).read_bytes().count(b"\r") == 0
    text = json.dumps(normalize(doc), indent=2, ensure_ascii=False) + "\n"
    golden("run_results/shop.json", text.replace(root.as_posix(), "<project>"))


def test_ephemeral_is_computed_once_and_released(tmp_path: Path) -> None:
    counter = tmp_path / "calls.txt"
    make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: p\npython_paths: [src]\n"
                f"vars: {{counter: {json.dumps(counter.as_posix())}}}\n"
                "targets:\n  ci:\n    " + engine_block(tmp_path, tables_for(tmp_path)) + "\n"
                "    catalog: {kind: memory}\n"
            ),
            "models/m.yml": (
                "sources:\n  - {name: raw, format: memory, location: raw/orders}\n"
                "models:\n"
                "  - {name: eph, materialized: ephemeral, python: 'm:eph', inputs: {raw: {source: raw}}}\n"
                "  - {name: a, python: 'm:same', inputs: {eph: {ref: eph}}}\n"
                "  - {name: b, python: 'm:same', inputs: {eph: {ref: eph}}}\n"
            ),
            "src/m.py": (
                "def eph(ctx, raw):\n"
                "    with open(ctx.vars['counter'], 'a') as fh:\n"
                "        fh.write('x')\n"
                "    return raw\n\n"
                "def same(ctx, eph):\n    return eph\n"
            ),
        },
    )
    outcome, _ = run_project(tmp_path, workers=2)
    assert outcome.exit_code == 0
    assert counter.read_text() == "x"
    assert len(store(tmp_path)["main.a"]) == 3


def test_failure_skips_downstream_and_reports_the_error(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: p\npython_paths: [src]\ntargets:\n  ci:\n"
                "    engine: {kind: memory}\n    catalog: {kind: memory}\n"
            ),
            "models/m.yml": (
                "models:\n  - {name: bad, python: 'm:bad'}\n"
                "  - {name: child, sql: select * from bad, inputs: {bad: {ref: bad}}}\n"
                "  - {name: other, sql: select 1 as x}\n"
            ),
            "src/m.py": "def bad(ctx):\n    raise ValueError('nope')\n",
        },
    )
    outcome, sink = run_project(tmp_path)
    assert (outcome.status, outcome.exit_code) == ("failed", 1)
    assert [d.code for d in outcome.diagnostics] == ["PLX101"]
    rows = {r["unit_id"]: r for r in results(tmp_path)["results"]}
    assert rows["bad"]["status"] == "failed" and rows["bad"]["error"]["code"] == "PLX101"
    assert rows["bad"]["error"]["category"] == "permanent"
    assert rows["bad"]["error"]["trace_excerpt"][-1] == "ValueError: nope"
    assert (rows["child"]["status"], rows["child"]["skip_reason"]) == ("skipped", "upstream_failed")
    assert rows["other"]["status"] == "succeeded"
    failed = [e for e in sink.events if e.type == "unit.failed"]
    assert len(failed) == 1 and "ValueError: nope" in failed[0].data["traceback"]  # type: ignore[operator]
    done = next(e for e in sink.events if e.type == "run.completed")
    assert done.data["status"] == "failed" and len(done.data["failures"]) == 1  # type: ignore[arg-type]


def single(tmp_path: Path, src: str, *, extra: str = "") -> Path:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: p\npython_paths: [src]\ntargets:\n  ci:\n"
                "    " + engine_block(tmp_path, {}) + "\n    catalog: {kind: memory}\n"
            ),
            "models/m.yml": "models:\n  - {name: m, python: 'm:m'" + extra + "}\n",
            "src/m.py": src,
        },
    )
    return tmp_path


def test_wrong_return_type_is_plx101_with_a_hint(tmp_path: Path) -> None:
    outcome, _ = run_project(single(tmp_path, "def m(ctx):\n    return 'not rows'\n"))
    (d,) = outcome.diagnostics
    assert d.code == "PLX101" and d.hint is not None and "memory" in d.hint
    assert "returned a value its engine cannot use" in d.message


def test_async_callable_and_model_log(tmp_path: Path) -> None:
    src = "async def m(ctx):\n    ctx.log('hello', rows=2)\n    return [{'a': 1}]\n"
    outcome, sink = run_project(single(tmp_path, src))
    assert outcome.exit_code == 0 and store(tmp_path)["main.m"] == ({"a": 1},)
    (log,) = [e for e in sink.events if e.type == "model.log"]
    assert log.data == {"message": "hello", "level": "info", "fields": {"rows": 2}}
    assert log.unit_id == "m" and log.attempt == 1


def test_python_paths_are_removed_after_the_run(tmp_path: Path) -> None:
    before = list(sys.path)
    run_project(single(tmp_path, "def m(ctx):\n    return [{'a': 1}]\n"))
    assert sys.path == before and "m" not in sys.modules


def test_reusing_a_run_id_counts_attempts_and_merges_results(tmp_path: Path) -> None:
    root = shop(tmp_path)
    run_project(root, run_id="same")
    outcome, _ = run_project(root, run_id="same", select=("fct_orders",))
    assert outcome.exit_code == 0
    units = root / "target" / "runs" / "same" / "units"
    assert sorted(p.name for p in (units / "fct_orders").iterdir()) == [
        "attempt-1.json",
        "attempt-2.json",
    ]
    rows = {r["unit_id"]: r for r in results(root, "same")["results"]}
    assert rows["fct_orders"]["attempt"] == 2 and rows["dim_customers"]["attempt"] == 1
    assert len(rows) == 6


def test_full_refresh_turns_append_into_overwrite(tmp_path: Path) -> None:
    root = shop(tmp_path)
    run_project(root, run_id="a")
    run_project(root, run_id="b", select=("fct_orders",))
    assert len(store(root)["main.fct_orders"]) == 6
    run_project(root, run_id="c", select=("fct_orders",), full_refresh=True)
    assert len(store(root)["main.fct_orders"]) == 3
    row = results(root, "c")["results"][0]
    assert (row["strategy"], row["full_refresh"]) == ("overwrite", True)


def test_a_view_is_created_and_reads_through(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: p\ntargets:\n  ci:\n    "
                + engine_block(tmp_path, tables_for(tmp_path))
                + "\n    catalog: {kind: memory}\n"
            ),
            "models/m.yml": (
                "sources:\n  - {name: raw, format: memory, location: raw/orders}\n"
                "models:\n"
                "  - {name: v, materialized: view, sql: select id from raw, inputs: {raw: {source: raw}}}\n"
            ),
        },
    )
    outcome, sink = run_project(tmp_path)
    assert outcome.exit_code == 0
    (row,) = results(tmp_path)["results"]
    assert (row["materialization"], row["strategy"], row["schema_hash"]) == ("view", None, None)
    assert "model.written" in sink.types()


def test_only_ephemeral_selected_means_nothing_to_run(tmp_path: Path) -> None:
    root = shop(tmp_path)
    outcome, sink = run_project(root, select=("orders_enriched",))
    assert (outcome.status, outcome.exit_code, outcome.run_results) == ("succeeded", 0, None)
    assert "run.started" not in sink.types()


def test_a_project_without_a_catalog_is_reported(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: p\ntargets:\n  ci:\n    engine: {kind: memory}\n"
            ),
            "models/m.yml": "models:\n  - {name: m, sql: select 1 as x}\n",
        },
    )
    outcome, _ = run_project(tmp_path)
    assert outcome.exit_code == 1 and [d.code for d in outcome.diagnostics] == ["PLS105"]


def test_a_failing_write_is_transient(tmp_path: Path) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: p\ntargets:\n  ci:\n"
                "    engine: {kind: memory}\n    catalog: {kind: failing}\n"
            ),
            "models/m.yml": "models:\n  - {name: m, sql: select 1 as x}\n",
        },
    )
    project = Project(
        ProjectOptions(
            project_dir=tmp_path, target="ci", plugins=("tests.runtime.fakes:FAILING",), env={}
        )
    )
    outcome = project.run(RunOptions(run_id="r"))
    assert outcome.exit_code == 1
    (row,) = results(tmp_path, "r")["results"]
    assert row["error"]["code"] == "PLM201" and row["error"]["category"] == "transient"
