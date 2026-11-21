"""on_cursor_value_missing on the memory engine (ADR-0027 amendment 1.3)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from pipeloom.testing.memory import MemoryEngine
from tests.runtime.test_incremental import engine, ids, project, put, run, stored

__all__ = ["engine"]

FIRST = [{"id": 1, "ts": 1}, {"id": 2, "ts": 2}, {"id": 3, "ts": None}]
SECOND = [*FIRST, {"id": 4, "ts": 4}, {"id": 5, "ts": None}]
MERGE = "    write: {strategy: merge, unique_key: [id]}\n"


def setup(tmp_path: Path, mode: str | None, write: str = "") -> Path:
    extra = f", on_cursor_value_missing: {mode}" if mode else ""
    return project(tmp_path, column="ts", extra=extra, write=write)


def test_unset_drops_null_rows_once_a_watermark_exists_and_reports_nothing(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = setup(tmp_path, None)
    put(engine, FIRST)
    assert run(root, "r1")[0].exit_code == 0 and ids(engine) == [1, 2, 3]  # no filter yet
    put(engine, SECOND)
    outcome, _, row = run(root, "r2")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3, 4]
    assert "rows_null_cursor" not in row and row["warnings"] == []


def test_raise_fails_the_unit_with_plm210_and_writes_nothing(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = setup(tmp_path, "raise")
    put(engine, FIRST)
    outcome, _, row = run(root, "r1")
    assert outcome.exit_code != 0 and "main.m" not in engine.store
    assert row["error"]["code"] == "PLM210"
    assert "has 1 rows with a null 'ts'" in row["error"]["message"]
    assert stored(root) is None


def test_raise_passes_when_no_cursor_is_null(tmp_path: Path, engine: MemoryEngine) -> None:
    root = setup(tmp_path, "raise")
    put(engine, FIRST[:2])
    assert run(root, "r1")[0].exit_code == 0 and ids(engine) == [1, 2]


def test_include_keeps_null_rows_in_every_batch_and_the_watermark_ignores_them(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = setup(tmp_path, "include", MERGE)
    put(engine, FIRST)
    assert run(root, "r1")[0].exit_code == 0 and ids(engine) == [1, 2, 3]
    put(engine, SECOND)
    outcome, _, row = run(root, "r2")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 3, 4, 5]
    assert row["rows_read"] == {"raw": 3}  # row 4 is after the watermark 2, rows 3 and 5 are null
    assert (row["watermark_before"], row["watermark_after"]) == (2, 4)
    outcome, _, row = run(root, "r3")  # nothing new: only the null rows come again
    assert row["rows_read"] == {"raw": 2} and row["watermark_after"] == 4


def test_exclude_drops_counts_and_warns(tmp_path: Path, engine: MemoryEngine) -> None:
    root = setup(tmp_path, "exclude")
    put(engine, FIRST)
    outcome, sink, row = run(root, "r1")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2]  # the first batch drops them too
    assert row["rows_null_cursor"] == 1
    assert row["warnings"] == ["null_cursor_excluded: 1 rows"]
    put(engine, SECOND)
    outcome, sink, row = run(root, "r2")
    assert outcome.exit_code == 0 and ids(engine) == [1, 2, 4]
    assert row["rows_null_cursor"] == 2 and row["rows_read"] == {"raw": 1}
    logs: list[dict[str, Any]] = [e.data for e in sink.events if e.type == "model.log"]
    assert any(d["level"] == "warning" and d["fields"] == {"rows_null_cursor": 2} for d in logs)


def test_exclude_without_nulls_records_zero_and_no_warning(
    tmp_path: Path, engine: MemoryEngine
) -> None:
    root = setup(tmp_path, "exclude")
    put(engine, FIRST[:2])
    _, _, row = run(root, "r1")
    assert row["rows_null_cursor"] == 0 and row["warnings"] == []


def test_the_setting_is_a_known_choice() -> None:
    from pipeloom.config.models import IncrementalConfig

    with pytest.raises(ValueError):
        IncrementalConfig(input="raw", column="ts", on_cursor_value_missing="skip")  # type: ignore[arg-type]
