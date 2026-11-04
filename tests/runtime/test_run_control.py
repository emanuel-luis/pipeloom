"""Run history on a reused run id, interrupts and unexpected errors."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from pipeloom.runtime import runner
from pipeloom.runtime.executors.thread import ThreadExecutor
from pipeloom.state.sqlite import SqliteStore
from tests.conftest import make_project
from tests.runtime.test_runner import results, run_project, shop

HEAD = "config_version: 1\nname: p\npython_paths: [src]\ntargets:\n  ci:\n"
ENGINE = "    engine: {kind: memory}\n    catalog: {kind: memory}\n"


def chain(tmp_path: Path) -> Path:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": HEAD + ENGINE,
            "models/m.yml": (
                "models:\n  - {name: a, sql: select 1 as x}\n"
                "  - {name: b, sql: select * from a, inputs: {a: {ref: a}}}\n"
            ),
        },
    )
    return tmp_path


def statuses(project_dir: Path) -> dict[str, str]:
    return {r["unit_id"]: r["status"] for r in results(project_dir)["results"]}


def test_a_reused_run_id_keeps_its_history(tmp_path: Path) -> None:
    root = shop(tmp_path)
    run_project(root, run_id="same", invocation_id="inv1", hour=10)
    run_project(root, run_id="same", invocation_id="inv2", hour=11, select=("fct_orders",))
    doc = results(root, "same")["run"]
    assert doc["invocation_ids"] == ["inv1", "inv2"]
    assert doc["started_at"] == "2026-01-01T10:00:00.000000Z"
    state = SqliteStore(root / ".pipeloom" / "state.db")
    record = state.get_run("same")
    assert record is not None and record.invocation_ids == ("inv1", "inv2")
    assert record.started_at == datetime(2026, 1, 1, 10, tzinfo=UTC)
    state.close()


def test_ctrl_c_stops_dequeuing_and_keeps_finished_units(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real = ThreadExecutor.wait_any
    calls: list[int] = []

    def wait_any(self: ThreadExecutor) -> Any:
        calls.append(1)
        if len(calls) == 1:
            raise KeyboardInterrupt
        return real(self)

    monkeypatch.setattr(ThreadExecutor, "wait_any", wait_any)
    outcome, sink = run_project(chain(tmp_path))
    assert (outcome.status, outcome.exit_code) == ("cancelled", 130)
    assert statuses(tmp_path) == {"a": "succeeded", "b": "cancelled"}
    assert [e.unit_id for e in sink.events if e.type == "unit.cancelled"] == ["b"]


def test_a_second_ctrl_c_propagates_after_finalizing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def wait_any(self: ThreadExecutor) -> Any:
        raise KeyboardInterrupt

    monkeypatch.setattr(ThreadExecutor, "wait_any", wait_any)
    with pytest.raises(KeyboardInterrupt):
        run_project(chain(tmp_path))
    assert statuses(tmp_path) == {"a": "succeeded", "b": "cancelled"}


def test_an_unexpected_error_ends_the_unit_as_failed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("bug")

    monkeypatch.setattr(runner.ModelRunner, "_compute", broken)
    outcome, sink = run_project(chain(tmp_path))
    assert outcome.exit_code == 1
    rows = {r["unit_id"]: r for r in results(tmp_path)["results"]}
    assert rows["a"]["error"]["code"] == "PLX101" and "pipeloom bug" in rows["a"]["error"]["hint"]
    assert rows["b"]["skip_reason"] == "upstream_failed"
    assert sink.types().count("unit.failed") == 1


def test_a_unit_whose_attempt_cannot_be_saved_still_gets_a_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def no_write(path: Path, data: Any) -> int:
        raise OSError("read-only")

    monkeypatch.setattr(runner, "write_json_atomic", no_write)
    with pytest.raises(OSError, match="read-only"):
        run_project(chain(tmp_path))
    rows = {r["unit_id"]: r for r in results(tmp_path)["results"]}
    assert rows["a"]["status"] == "failed" and rows["a"]["error"]["code"] == "PLX101"
    assert rows["b"]["status"] == "skipped"


def test_a_relative_project_dir_does_not_leak_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_project(
        tmp_path,
        {
            "pipeloom.yml": HEAD + ENGINE,
            "models/m.yml": "models:\n  - {name: m, python: 'm:m'}\n",
            "src/m.py": "def m(ctx):\n    raise ValueError('boom')\n",
        },
    )
    monkeypatch.chdir(tmp_path)
    outcome, sink = run_project(Path("."))
    assert outcome.exit_code == 1
    (failed,) = [e for e in sink.events if e.type == "unit.failed"]
    trace = str(failed.data["traceback"])
    flat = trace.replace("\\", "/")
    assert "ValueError: boom" in trace and "src/m.py" in flat
    assert tmp_path.as_posix() not in flat and str(tmp_path) not in trace
    location = outcome.diagnostics[0].location
    assert location is not None and location.file == "src/m.py"
