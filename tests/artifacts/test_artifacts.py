from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeloom.artifacts.finalize import finalize
from pipeloom.artifacts.io import write_json_atomic
from pipeloom.artifacts.layout import RunPaths
from pipeloom.artifacts.results import (
    RunHeader,
    build_run_results,
    result_row,
    schema_hash,
    status_of,
)


def test_run_paths(tmp_path: Path) -> None:
    p = RunPaths(tmp_path, "r1")
    assert p.run_dir == tmp_path / "runs" / "r1"
    assert p.attempt("u", 2) == p.run_dir / "units" / "u" / "attempt-2.json"
    assert p.log("u", 1).name == "u-attempt-1.log" and p.events("inv").suffix == ".jsonl"
    assert p.next_attempt("u") == 1
    write_json_atomic(p.attempt("u", 1), {})
    write_json_atomic(p.attempt("u", 3), {})
    assert p.next_attempt("u") == 4


def test_write_json_atomic_is_lf_utf8_and_leaves_no_temp_files(tmp_path: Path) -> None:
    path = tmp_path / "a" / "x.json"
    n = write_json_atomic(path, {"b": "café", "a": [1]})
    raw = path.read_bytes()
    assert n == len(raw) and b"\r" not in raw and raw.endswith(b"}\n")
    assert json.loads(raw.decode("utf-8")) == {"b": "café", "a": [1]}
    assert raw.decode("utf-8").startswith('{\n  "b"')
    write_json_atomic(path, {"v": 2})
    assert [p.name for p in path.parent.iterdir()] == ["x.json"]
    with pytest.raises(ValueError):
        write_json_atomic(path, {"v": float("nan")})
    assert [p.name for p in path.parent.iterdir()] == ["x.json"]


def test_result_row_has_every_field_and_rejects_unknown_ones() -> None:
    row = result_row(unit_id="u", status="succeeded")
    assert row["schema_changes"] == [] and row["rows_dropped"] is None and row["retries_used"] == 0
    row["warnings"].append("x")
    assert result_row()["warnings"] == []
    with pytest.raises(KeyError):
        result_row(nope=1)


def test_schema_hash_depends_on_columns_and_order() -> None:
    a = schema_hash([("x", "int64", True), ("y", "string", True)])
    assert a == schema_hash([("x", "int64", True), ("y", "string", True)])
    assert a != schema_hash([("y", "string", True), ("x", "int64", True)])
    assert len(a) == 64


def header() -> RunHeader:
    return RunHeader(
        run_id="r",
        invocation_id="i",
        invocation_ids=("h", "i"),
        project="p",
        target="t",
        plan_hash="h",
        pipeloom_version="0",
        plugins={},
        args={},
        selector={},
        full_refresh=False,
        generated_at="g",
        started_at="s",
        ended_at="e",
        duration_ms=1,
        identity={},
        python_version="3",
        git_sha=None,
    )


def test_status_and_totals() -> None:
    ok = result_row(unit_id="a", status="succeeded", rows_written=3, bytes_written=None)
    assert status_of([ok]) == "succeeded"
    assert status_of([ok, result_row(unit_id="b", status="skipped")]) == "failed"
    assert status_of([ok, result_row(unit_id="b", status="cancelled")]) == "cancelled"
    doc = build_run_results(header(), [result_row(unit_id="b", status="failed"), ok])
    assert [r["unit_id"] for r in doc["results"]] == ["a", "b"]
    assert doc["run"]["totals"] == {"rows_written": 3, "bytes_written": 0}
    assert doc["run"]["invocation_ids"] == ["h", "i"]
    assert doc["run"]["exit_code"] == 1 and doc["schema"] == "pipeloom.run_results/1"


def test_finalize_prefers_scheduler_verdicts_and_copies_to_the_root(tmp_path: Path) -> None:
    paths = RunPaths(tmp_path / "target", "r")
    write_json_atomic(paths.manifest, {"m": 1})
    write_json_atomic(paths.attempt("a", 1), result_row(unit_id="a", status="failed"))
    write_json_atomic(paths.attempt("a", 2), result_row(unit_id="a", status="succeeded", attempt=2))
    write_json_atomic(paths.attempt("old", 1), result_row(unit_id="old", status="succeeded"))
    nodes = {"a": "model.p.a", "b": "model.p.b", "c": "model.p.c", "old": "model.p.old"}
    done = finalize(
        paths,
        header(),
        unit_nodes=nodes,
        skipped={"b": ("upstream_failed", "a")},
        cancelled=["c"],
    )
    rows = {r["unit_id"]: r for r in done.run_results["results"]}
    assert rows["a"]["attempt"] == 2 and rows["old"]["status"] == "succeeded"
    assert (rows["b"]["status"], rows["b"]["skip_reason"]) == ("skipped", "upstream_failed")
    assert rows["c"]["status"] == "cancelled" and rows["b"]["unique_id"] == "model.p.b"
    assert (tmp_path / "target" / "run_results.json").read_bytes() == paths.run_results.read_bytes()
    assert (tmp_path / "target" / "manifest.json").is_file()
