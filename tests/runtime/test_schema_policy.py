"""The on_schema_change policy table, and runs that apply it."""

from __future__ import annotations

import io
import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from rich.console import Console

from pipeloom.api import Project, ProjectOptions
from pipeloom.engine.types import Column, Schema
from pipeloom.events.bus import EventBus
from pipeloom.events.sinks.console import ConsoleSink
from pipeloom.runtime.run import RunOptions
from pipeloom.runtime.schema_policy import SchemaRejected, resolve_columns
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import JsonTableStore
from pipeloom.testing.recording import RecordingSink
from tests.conftest import make_project

TARGET: Schema = (Column("id", "int64"), Column("v", "int64"), Column("s", "string"))


def names(cols: Schema) -> list[tuple[str, str]]:
    return [(c.name, c.dtype) for c in cols]


ADDED: Schema = (*TARGET, Column("x", "string"))
REMOVED: Schema = TARGET[:2]
WIDER: Schema = (Column("id", "int64"), Column("v", "float64"), Column("s", "string"))
BAD: Schema = (Column("id", "int64"), Column("v", "string"), Column("s", "string"))
NARROW: Schema = (Column("id", "int32"), Column("v", "int64"), Column("s", "string"))

# policy, batch, expected columns (None = rejected), expected action
TABLE: list[tuple[str, Schema, Schema | None, str]] = [
    ("fail", TARGET, TARGET, "none"),
    ("fail", NARROW, TARGET, "none"),
    ("fail", ADDED, None, "rejected"),
    ("fail", REMOVED, None, "rejected"),
    ("fail", WIDER, None, "rejected"),
    ("fail", BAD, None, "rejected"),
    ("ignore", ADDED, TARGET, "ignored"),
    ("ignore", REMOVED, TARGET, "ignored"),
    ("ignore", WIDER, TARGET, "ignored"),
    ("ignore", BAD, TARGET, "ignored"),
    ("append_new_columns", ADDED, ADDED, "applied"),
    ("append_new_columns", REMOVED, TARGET, "ignored"),
    ("append_new_columns", WIDER, None, "rejected"),
    ("append_new_columns", BAD, None, "rejected"),
    ("sync_all_columns", ADDED, ADDED, "applied"),
    ("sync_all_columns", REMOVED, REMOVED, "applied"),
    ("sync_all_columns", WIDER, WIDER, "applied"),
    ("sync_all_columns", BAD, None, "rejected"),
    ("sync_all_columns", NARROW, TARGET, "none"),
]


@pytest.mark.parametrize("strategy", ["append", "merge", "replace_partitions"])
@pytest.mark.parametrize(("policy", "batch", "want", "action"), TABLE)
def test_policy_table(
    policy: str, batch: Schema, want: Schema | None, action: str, strategy: str
) -> None:
    if want is None:
        with pytest.raises(SchemaRejected) as caught:
            resolve_columns(policy, TARGET, batch, strategy=strategy, full_refresh=False)
        assert caught.value.policy == policy and not caught.value.diff.empty
        return
    columns, diff, got = resolve_columns(
        policy, TARGET, batch, strategy=strategy, full_refresh=False
    )
    assert names(columns) == names(want) and got == action
    assert diff.empty == (action == "none")


@pytest.mark.parametrize("policy", ["fail", "ignore", "append_new_columns", "sync_all_columns"])
def test_overwrite_full_refresh_and_first_build_replace_the_schema(policy: str) -> None:
    for strategy, full_refresh, target, action in (
        ("overwrite", False, TARGET, "replaced"),
        ("append", True, TARGET, "replaced"),
        ("append", False, None, "none"),
    ):
        columns, diff, got = resolve_columns(
            policy, target, BAD, strategy=strategy, full_refresh=full_refresh
        )
        assert columns == BAD and got == action
        assert diff.empty == (target is None)


def test_added_columns_are_nullable_and_keep_batch_order() -> None:
    batch = (*TARGET, Column("y", "int64", nullable=False), Column("x", "string"))
    columns, _, _ = resolve_columns(
        "append_new_columns", TARGET, batch, strategy="append", full_refresh=False
    )
    assert [c.name for c in columns] == ["id", "v", "s", "y", "x"]
    assert all(c.nullable for c in columns[3:])


def test_the_rejection_message_lists_the_changes() -> None:
    batch = (*BAD, Column("x", "string"))
    with pytest.raises(SchemaRejected, match=r"v: int64 -> string, added x \(string\)"):
        resolve_columns("fail", TARGET, batch, strategy="append", full_refresh=False)


def test_an_unknown_policy_is_a_bug() -> None:
    with pytest.raises(ValueError, match="unknown"):
        resolve_columns("nope", TARGET, TARGET, strategy="append", full_refresh=False)


def _project(tmp_path: Path, policy: str, sql: str, strategy: str = "append") -> Path:
    return make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: shop\npython_paths: [src]\n"
                "targets:\n  dev:\n"
                "    engine: {kind: memory, root: store, tables: {main.a: [{id: 1, v: 10}]}}\n"
                "    catalog: {kind: memory, root: store}\n"
                "    naming: {kind: none}\n"
            ),
            "models/m.yml": (
                "models:\n"
                f"  - name: a\n    sql: {sql}\n"
                f"    write: {{strategy: {strategy}, on_schema_change: {policy}}}\n"
            ),
        },
    )


def _run(root: Path, *sinks: Any) -> Any:
    clock = FakeClock(datetime(2026, 1, 1, 14, 2, 11, tzinfo=UTC), step=timedelta(0))
    project = Project(
        ProjectOptions(project_dir=root, env={"PIPELOOM_USER": "tester"}),
        clock=clock,
        invocation_id="inv",
    )
    with EventBus([(s, 5) for s in sinks], clock=clock, invocation_id="inv") as bus:
        return project.run(RunOptions(run_id="r1", workers=1), bus)


def _row(root: Path) -> dict[str, Any]:
    path = root / "target" / "runs" / "r1" / "run_results.json"
    (row,) = json.loads(path.read_bytes())["results"]
    out: dict[str, Any] = row
    return out


def test_append_new_columns_run(tmp_path: Path, golden: Callable[[str, str], None]) -> None:
    root = _project(tmp_path, "append_new_columns", "select 2 as id, 20 as v, 'n' as extra")
    sink, out = RecordingSink(), io.StringIO()
    console = ConsoleSink(Console(file=out, width=120, color_system=None), tz=UTC)
    outcome = _run(root, sink, console)
    assert outcome.exit_code == 0
    assert list(JsonTableStore(root / "store")["main.a"]) == [
        {"id": 1, "v": 10, "extra": None},
        {"id": 2, "v": 20, "extra": "n"},
    ]
    (event,) = [e for e in sink.events if e.type == "model.schema_changed"]
    golden(
        "events/model_schema_changed.json",
        json.dumps(event.data, indent=2, ensure_ascii=True) + "\n",
    )
    assert event.model == "a" and event.level.value == "info"
    line = "schema changed on main.a (policy append_new_columns, applied): +extra string"
    assert line in out.getvalue()
    assert _row(root)["schema_changes"] == [
        {"kind": "added", "column": "extra", "old": None, "new": "string", "action": "applied"}
    ]


def test_ignore_run_casts_to_the_target(tmp_path: Path) -> None:
    root = _project(tmp_path, "ignore", "select 2 as id, 'junk' as extra")
    sink = RecordingSink()
    assert _run(root, sink).exit_code == 0
    assert list(JsonTableStore(root / "store")["main.a"]) == [
        {"id": 1, "v": 10},
        {"id": 2, "v": None},
    ]
    (event,) = [e for e in sink.events if e.type == "model.schema_changed"]
    assert event.data["action"] == "ignored"


def test_sync_all_columns_run_drops_and_adds(tmp_path: Path) -> None:
    root = _project(tmp_path, "sync_all_columns", "select 2 as id, 'n' as extra")
    assert _run(root).exit_code == 0
    assert list(JsonTableStore(root / "store")["main.a"]) == [
        {"id": 1, "extra": None},
        {"id": 2, "extra": "n"},
    ]


def test_a_rejected_change_fails_the_unit_and_leaves_the_table(tmp_path: Path) -> None:
    root = _project(tmp_path, "fail", "select 2 as id, 20 as v, 'n' as extra")
    sink = RecordingSink()
    assert _run(root, sink).exit_code == 1
    assert list(JsonTableStore(root / "store")["main.a"]) == [{"id": 1, "v": 10}]
    (event,) = [e for e in sink.events if e.type == "model.schema_changed"]
    assert event.data["action"] == "rejected" and event.level.value == "warning"
    row = _row(root)
    assert row["error"]["code"] == "PLM202" and row["schema_changes"][0]["action"] == "rejected"


def test_overwrite_reports_replaced(tmp_path: Path) -> None:
    root = _project(tmp_path, "fail", "select 2 as id, 'n' as extra", "overwrite")
    sink = RecordingSink()
    assert _run(root, sink).exit_code == 0
    (event,) = [e for e in sink.events if e.type == "model.schema_changed"]
    assert event.data["action"] == "replaced"
    assert list(JsonTableStore(root / "store")["main.a"]) == [{"id": 2, "extra": "n"}]
