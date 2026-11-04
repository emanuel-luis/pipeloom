"""Loader, ephemeral memo and error classification."""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

from pipeloom.diagnostics import Location
from pipeloom.engine.types import Relation
from pipeloom.runtime.errors import build_error, classify, to_diagnostic
from pipeloom.runtime.loader import CallableLoader
from pipeloom.runtime.memo import EphemeralMemo

REL = Relation((), "memory")


def test_loader_prepends_and_removes_only_its_paths(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "loader_mod.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    before = list(sys.path)
    with CallableLoader(tmp_path, ["src"]) as loader:
        assert sys.path[0] == str((tmp_path / "src").resolve())
        assert loader.load("loader_mod:f")() == 1
        assert loader.load("loader_mod:f") is loader.load("loader_mod:f")
        with pytest.raises(AttributeError):
            loader.load("loader_mod:missing")
    assert sys.path == before and "loader_mod" not in sys.modules


def test_memo_computes_once_for_concurrent_consumers() -> None:
    memo = EphemeralMemo({"e": 2})
    calls: list[int] = []
    start = threading.Barrier(2)

    def compute() -> Relation:
        calls.append(1)
        return REL

    def consume(unit: str) -> None:
        start.wait()
        assert memo.get("e", compute, unit_id=unit) is REL

    threads = [threading.Thread(target=consume, args=(u,)) for u in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert calls == [1] and len(memo) == 1
    memo.release("a")
    assert len(memo) == 1
    memo.release("b")
    assert len(memo) == 0


def test_memo_persists_shared_results_and_caches_failures() -> None:
    persisted: list[Relation] = []

    def persist(rel: Relation) -> Relation:
        persisted.append(rel)
        return rel

    memo = EphemeralMemo({"shared": 2, "solo": 1, "bad": 2}, persist=persist)
    memo.get("shared", lambda: REL, unit_id="a")
    memo.get("solo", lambda: REL, unit_id="a")
    assert len(persisted) == 1

    def boom() -> Relation:
        raise ValueError("once")

    for unit in ("a", "b"):
        with pytest.raises(ValueError, match="once"):
            memo.get("bad", boom, unit_id=unit)


def test_classification() -> None:
    assert classify(OSError("x"), "read") == "transient"
    assert classify(OSError("x"), "write") == "transient"
    assert classify(OSError("x"), "body") == "permanent"
    assert classify(ValueError("x"), "write") == "permanent"


def raiser() -> None:
    raise KeyError("deep")


def test_error_info_falls_back_when_no_frame_is_in_the_project(tmp_path: Path) -> None:
    try:
        raiser()
    except KeyError as exc:
        err = build_error(
            "PLX101",
            exc,
            phase="body",
            message="model 'm' raised",
            hint="h",
            project_dir=tmp_path,
            fallback=Location("models/m.yml", 3),
        )
    assert err.location == Location("models/m.yml", 3).to_json()
    assert err.trace_excerpt == ["KeyError: 'deep'"] and err.category == "permanent"
    assert err.message == "model 'm' raised: KeyError: 'deep'"
    diag = to_diagnostic(err, unit_id="m")
    assert diag.code == "PLX101" and diag.context == {"unit": "m", "category": "permanent"}


def test_error_info_uses_the_innermost_project_frame() -> None:
    here = Path(__file__).parent
    try:
        raiser()
    except KeyError as exc:
        err = build_error(
            "PLX101", exc, phase="body", message="m", hint=None, project_dir=here, fallback=None
        )
    assert err.location is not None and err.location["file"] == "test_support.py"
    assert "in raiser" in err.trace_excerpt[-2] and err.trace_excerpt[-2].startswith("test_support")
