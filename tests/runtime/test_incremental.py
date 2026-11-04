"""Incremental models on the memory engine: the watermark, its CAS and the token replay."""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.runtime.incremental import IncrementalState, family, next_watermark
from pipeloom.runtime.run import RunOptions
from pipeloom.state.sqlite import SqliteStore
from pipeloom.state.types import StateKey, Watermark
from pipeloom.testing.clock import FakeClock
from pipeloom.testing.memory import MemoryEngine
from pipeloom.testing.recording import RecordingSink
from tests.conftest import make_project
from tests.runtime.fakes import FAULT, reset_keep

KEY = StateKey("inc", "dev", "m")
T = datetime(2026, 1, 10, tzinfo=UTC)
PLUGINS = (
    "tests.runtime.fakes:KEEP_ENGINE",
    "tests.runtime.fakes:KEEP_CATALOG",
    "tests.runtime.fakes:FAULTY",
)
MODEL = """\
from typing import Any


def m(ctx: Any, raw: Any) -> Any:
    ctx.log(
        "ctx",
        is_incremental=ctx.is_incremental,
        watermark=str(ctx.watermark),
        lower_bound=str(ctx.lower_bound),
    )
    return raw
"""


@pytest.fixture
def engine() -> Iterator[MemoryEngine]:
    yield reset_keep()
    FAULT.update(crash=False, move=False)


def project(tmp_path: Path, *, column: str = "id", extra: str = "", write: str = "") -> Path:
    return make_project(
        tmp_path,
        {
            "pipeloom.yml": (
                "config_version: 1\nname: inc\npython_paths: [src]\n"
                "targets:\n  dev:\n    engine: {kind: keepmem}\n    catalog: {kind: keepmem}\n"
                "    state: {kind: faulty}\n    naming: {kind: none}\n"
            ),
            "models/m.yml": (
                "models:\n  - name: m\n    materialized: incremental\n"
                "    python: inc.models:m\n    inputs: {raw: {source: raw}}\n"
                f"    incremental: {{input: raw, column: {column}{extra}}}\n"
                f"{write}"
            ),
            "models/sources.yml": "sources:\n  - {name: raw, table: main.raw}\n",
            "src/inc/__init__.py": "",
            "src/inc/models.py": MODEL,
        },
    )


def run(
    root: Path, run_id: str = "r1", *, full_refresh: bool = False
) -> tuple[Any, RecordingSink, dict[str, Any]]:
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    sink = RecordingSink()
    proj = Project(
        ProjectOptions(
            project_dir=root, target="dev", plugins=PLUGINS, env={"PIPELOOM_USER": "tester"}
        ),
        clock=clock,
        invocation_id="inv",
    )
    with EventBus([(sink, 5)], clock=clock, invocation_id="inv") as bus:
        outcome = proj.run(RunOptions(run_id=run_id, workers=1, full_refresh=full_refresh), bus)
    path = root / "target" / "runs" / run_id / "run_results.json"
    row: dict[str, Any] = {}
    if path.exists():
        (row,) = json.loads(path.read_bytes())["results"]
    return outcome, sink, row


def put(engine: MemoryEngine, rows: list[dict[str, Any]], name: str = "main.raw") -> None:
    engine.store[name] = tuple(rows)


def ids(engine: MemoryEngine) -> list[int]:
    return sorted(r["id"] for r in engine.store["main.m"])


def stored(root: Path) -> Watermark | None:
    return SqliteStore(root / ".pipeloom" / "state.db").get_watermark(KEY)


def ctx_log(sink: RecordingSink) -> dict[str, Any]:
    (event,) = [e for e in sink.events if e.type == "model.log"]
    fields: dict[str, Any] = event.data["fields"]
    return fields


def test_first_run_is_full_and_the_second_reads_only_new_rows(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = project(tmp_path)
    put(engine, [{"id": i} for i in (1, 2, 3)])
    outcome, sink, row = run(root, "r1")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3]
    assert row["is_incremental"] is False and row["watermark_before"] is None
    assert row["watermark_after"] == 3 and row["strategy"] == "append"
    assert ctx_log(sink)["is_incremental"] is False
    wm = stored(root)
    assert wm is not None and (wm.value, wm.dtype, wm.run_id) == (3, "int64", "r1")

    put(engine, [{"id": i} for i in range(1, 6)])
    outcome, sink, row = run(root, "r2")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3, 4, 5]
    assert row["is_incremental"] is True and row["rows_read"] == {"raw": 2}
    assert (row["watermark_before"], row["watermark_after"]) == (3, 5)
    fields = ctx_log(sink)
    assert fields["watermark"] == "3" and fields["lower_bound"] == "3"
    (event,) = [e for e in sink.events if e.type == "state.watermark_committed"]
    assert event.data == {
        "column": "id",
        "dtype": "int64",
        "before": 3,
        "after": 5,
        "source": "state",
        "seeded": False,
    }


def test_lookback_reprocesses_the_window_and_never_moves_the_watermark_back(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = project(
        tmp_path,
        column="ts",
        extra=", lookback: 2d",
        write="    write: {strategy: merge, unique_key: [id]}\n",
    )
    day = timedelta(days=1)
    put(engine, [{"id": 1, "ts": T - 5 * day}, {"id": 2, "ts": T}])
    assert run(root, "r1")[0].exit_code == 0
    put(
        engine,
        [
            {"id": 1, "ts": T - 5 * day},
            {"id": 2, "ts": T},
            {"id": 3, "ts": T - day},
            {"id": 4, "ts": T - 3 * day},
            {"id": 5, "ts": T + day},
        ],
    )
    outcome, sink, row = run(root, "r2")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3, 5]  # 4 is older than the window
    assert row["rows_read"] == {"raw": 3} and ctx_log(sink)["lower_bound"] == str(T - 2 * day)
    wm = stored(root)
    assert wm is not None and wm.value == T + day

    put(engine, [{"id": 9, "ts": T}])  # inside the window and below the watermark
    outcome, _, row = run(root, "r3")
    assert outcome.exit_code == 0 and 9 in ids(engine)
    wm = stored(root)
    assert (
        wm is not None and wm.value == T + day and row["watermark_after"] == (T + day).isoformat()
    )


def test_lookback_zero_is_strict_and_an_empty_batch_keeps_the_watermark(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    # the memory engine cannot tell the schema of an empty batch: ignore takes the target's
    root = project(tmp_path, write="    write: {on_schema_change: ignore}\n")
    put(engine, [{"id": 1}, {"id": 2}])
    assert run(root, "r1")[0].exit_code == 0
    outcome, _, row = run(root, "r2")  # id 2 equals the watermark: not read again
    assert outcome.exit_code == 0 and ids(engine) == [1, 2]
    assert row["rows_read"] == {"raw": 0} and row["rows_written"] == 0
    assert row["watermark_before"] == row["watermark_after"] == 2
    wm = stored(root)
    assert wm is not None and wm.value == 2


def test_the_watermark_is_seeded_from_the_target_after_the_state_is_wiped(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = project(tmp_path)
    put(engine, [{"id": 1}, {"id": 2}])
    assert run(root, "r1")[0].exit_code == 0
    (root / ".pipeloom" / "state.db").unlink()
    put(engine, [{"id": i} for i in (1, 2, 3)])
    outcome, sink, row = run(root, "r2")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3]
    assert row["is_incremental"] is True and row["warnings"] == ["watermark_seeded: target"]
    (event,) = [e for e in sink.events if e.type == "state.watermark_committed"]
    assert event.data["seeded"] is True and event.data["before"] == 2 and event.data["after"] == 3
    wm = stored(root)
    assert wm is not None and wm.value == 3


def test_watermark_source_target_writes_no_state(tmp_path: Path, engine: MemoryEngine) -> None:
    root = project(tmp_path, extra=", watermark_source: target")
    put(engine, [{"id": 1}, {"id": 2}])
    assert run(root, "r1")[0].exit_code == 0
    put(engine, [{"id": i} for i in (1, 2, 3)])
    outcome, sink, row = run(root, "r2")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3] and row["is_incremental"] is True
    assert stored(root) is None
    assert not [e for e in sink.events if e.type == "state.watermark_committed"]


def test_full_refresh_overwrites_and_commits_again(tmp_path: Path, engine: MemoryEngine) -> None:
    root = project(tmp_path)
    put(engine, [{"id": 1}, {"id": 2}])
    assert run(root, "r1")[0].exit_code == 0
    put(engine, [{"id": i} for i in (1, 2, 3, 4)])
    outcome, _, row = run(root, "r2", full_refresh=True)
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3, 4]
    assert row["strategy"] == "overwrite" and row["full_refresh"] is True
    assert row["is_incremental"] is False and row["watermark_after"] == 4
    wm = stored(root)
    assert wm is not None and wm.value == 4


def test_a_watermark_that_moved_during_the_run_is_plm203_and_the_data_stays(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = project(tmp_path)
    put(engine, [{"id": 1}])
    assert run(root, "r1")[0].exit_code == 0
    put(engine, [{"id": 1}, {"id": 2}])
    FAULT["move"] = True
    outcome, _, row = run(root, "r2")
    assert outcome.exit_code == 1 and row["error"]["code"] == "PLM203"
    assert row["error"]["category"] == "permanent" and ids(engine) == [1, 2]
    assert row["rows_written"] == 1


def test_a_crash_before_the_swap_replays_the_original_watermark(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = project(tmp_path)
    put(engine, [{"id": 1}])
    assert run(root, "r1")[0].exit_code == 0
    put(engine, [{"id": 1}, {"id": 2}])
    FAULT["crash"] = True
    outcome, _, row = run(root, "r2")
    assert outcome.exit_code == 1 and row["error"]["code"] == "PLX101"
    assert ids(engine) == [1, 2]  # written
    wm = stored(root)
    assert wm is not None and wm.value == 1  # not swapped

    FAULT["crash"] = False
    put(engine, [{"id": i} for i in (1, 2, 3)])  # new rows arrived since
    outcome, sink, row = run(root, "r2")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2]  # nothing read, nothing written
    assert row["watermark_after"] == 2 and row["rows_written"] == 1
    (written,) = [e for e in sink.events if e.type == "model.written"]
    assert written.data["skipped"] is True
    wm = stored(root)
    assert wm is not None and wm.value == 2 and wm.run_id == "r2"
    assert not [e for e in sink.events if e.type == "model.log"]  # the body did not run

    outcome, _, _ = run(root, "r3")  # the next run picks up row 3
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3]


def test_a_replay_after_the_swap_succeeds_too(tmp_path: Path, engine: MemoryEngine) -> None:
    root = project(tmp_path)
    put(engine, [{"id": 1}, {"id": 2}])
    assert run(root, "r1")[0].exit_code == 0
    again, _, row = run(root, "r1")  # the same run id: the store already holds the watermark
    assert again.exit_code == 0 and ids(engine) == [1, 2] and row["watermark_after"] == 2


def test_a_timezone_mismatch_is_plm205(tmp_path: Path, engine: MemoryEngine) -> None:
    root = project(tmp_path, column="ts")
    put(engine, [{"id": 1, "ts": T}])
    assert run(root, "r1")[0].exit_code == 0
    store = SqliteStore(root / ".pipeloom" / "state.db")
    naive = Watermark(datetime(2026, 1, 10), "timestamp", "x", T)
    assert store.cas_watermark(KEY, stored(root), naive)
    outcome, _, row = run(root, "r2")
    assert outcome.exit_code == 1 and row["error"]["code"] == "PLM205"


def test_lookback_on_an_integer_column_is_plm205(tmp_path: Path, engine: MemoryEngine) -> None:
    root = project(tmp_path, extra=", lookback: 1d")
    put(engine, [{"id": 1}])
    outcome, _, row = run(root, "r1")
    assert outcome.exit_code == 1 and row["error"]["code"] == "PLM205"


def test_a_missing_column_is_plm205(tmp_path: Path, engine: MemoryEngine) -> None:
    root = project(tmp_path, column="nope")
    put(engine, [{"id": 1}])
    outcome, _, row = run(root, "r1")
    assert outcome.exit_code == 1 and row["error"]["code"] == "PLM205"


@pytest.mark.parametrize(
    ("dtype", "want"),
    [
        ("int64", "integer"),
        ("uint8", "integer"),
        ("date", "date"),
        ("timestamp[us]", "timestamp"),
        ("timestamp[us, UTC]", "timestamp_tz"),
        ("timestamp[UTC]", "timestamp_tz"),
        ("timestamp", "timestamp"),
        ("decimal(18,2)", "decimal"),
        ("float64", "float"),
        ("string", "string"),
    ],
)
def test_family(dtype: str, want: str) -> None:
    assert family(dtype) == want


def _st(incremental: bool, w: Any) -> IncrementalState:
    return IncrementalState(incremental, w, None, True, "int64", None, False, "state")


def test_next_watermark() -> None:
    assert next_watermark(_st(True, 5), 3) == 5
    assert next_watermark(_st(True, 5), 8) == 8
    assert next_watermark(_st(True, 5), None) == 5
    assert next_watermark(_st(False, None), 8) == 8
    assert next_watermark(_st(False, None), None) is None
