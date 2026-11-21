"""Performance budget: a synthetic 1,000-model project must stay inside a time budget (roadmap 1.2).

Runs only with PIPELOOM_TEST_PERF=1 (one dedicated CI job on ubuntu, see ci.yml), because wall
time on a busy matrix cell is noise. PIPELOOM_TEST_PERF_BUDGET_SCALE multiplies every budget
(default 1); set it above 1 on a slow disk or laptop. Stages run in-process through the API.
"""

from __future__ import annotations

import os
import random
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from pipeloom.api import Project
from pipeloom.config.load import ProjectOptions
from pipeloom.discovery import Discovery
from pipeloom.plan.manifest import manifest_bytes
from tests.conftest import make_project

pytestmark = [
    pytest.mark.perf,
    pytest.mark.skipif(
        os.environ.get("PIPELOOM_TEST_PERF") != "1",
        reason="set PIPELOOM_TEST_PERF=1 to run the performance budget",
    ),
]

N = 1000
PER_FOLDER = 50
SOURCES = 100  # the first models read a source; later ones read 1 to 3 earlier models

# Seconds for N models. Quiet-machine times (WSL ubuntu) were 0.3, 0.9, 0.5, 0.2, 0.2 and 1.1;
# under heavy parallel load they were up to 5x that. Budgets sit near the loaded numbers, so
# noise passes and an accidental quadratic step (10x or more at N=1000) fails. Cold discovery
# keeps the 2 s ADR-0050 promise. Re-tune from the first green CI runs ("perf seconds" is printed).
BUDGETS = {
    "discovery_cold": 2.0,
    "resolve": 4.0,
    "validate": 3.0,
    "compile": 3.0,
    "compile_stable": 2.0,
    "compile_warm": 6.0,
    # "plan": filled in with the `plan --diff` change (budget for a 1-model change on N models)
}

CONFIG = """\
config_version: 1
name: perf
sources:
  raw:
    tables:
      raw:
        location: raw
targets:
  dev:
    engine:
      kind: memory
    catalog:
      kind: memory
default_target: dev
"""


def scale() -> float:
    return float(os.environ.get("PIPELOOM_TEST_PERF_BUDGET_SCALE", "1"))


def generate(root: Path, n: int = N) -> Path:
    """Write n static ctx.sql models over n // PER_FOLDER folders, a DAG with fan-in and fan-out
    (seeded, so every run is identical), and a properties file with data tests per 10th folder."""
    rng = random.Random(7)
    files: dict[str, str] = {"pipeloom.yml": CONFIG}
    for i in range(n):
        folder = f"models/f{i // PER_FOLDER:02d}"
        if i < min(SOURCES, n):
            ups: list[str] = []
            head = '@model(inputs={"src": source("raw", "raw")}, materialized="table")'
            args, sql = "ctx, src", "select id from src"
        else:
            # earlier models, biased to the first few so some have a large fan-out
            ups = sorted({f"m{int(rng.random() ** 2 * i):04d}" for _ in range(rng.randint(1, 3))})
            head = '@model(materialized="view", tags=["perf"])' if i % 3 else "@model"
            args, sql = ", ".join(["ctx", *ups]), f"select id from {ups[0]}"
        files[f"{folder}/m{i:04d}.py"] = (
            f"from pipeloom import model, source\n\n\n{head}\n"
            f"def m{i:04d}({args}):\n    return ctx.sql({sql!r})\n"
        )
    for f in range(0, n // PER_FOLDER, 10):
        names = [f"m{f * PER_FOLDER + k:04d}" for k in range(5)]
        files[f"models/f{f:02d}/_properties.yml"] = "models:\n" + "".join(
            f"  - name: {m}\n    data_tests:\n      - name: id_set\n"
            "        check:\n          not_null:\n            - id\n"
            for m in names
        )
    return make_project(root, files, convert=False)


def timed(stage: str, out: dict[str, float], fn: Callable[[], Any]) -> Any:
    start = time.perf_counter()
    value = fn()
    out[stage] = time.perf_counter() - start
    return value


def test_generator_is_deterministic_and_a_dag(tmp_path: Path) -> None:
    a, b = generate(tmp_path / "a", 120), generate(tmp_path / "b", 120)
    for p in sorted(a.rglob("*.py")):
        assert p.read_text() == (b / p.relative_to(a)).read_text()
    outcome = Project(ProjectOptions(project_dir=a, env={})).validate()
    assert outcome.value is not None, outcome.diagnostics


def test_budget_scale_defaults_to_one_and_reads_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PIPELOOM_TEST_PERF_BUDGET_SCALE", raising=False)
    assert scale() == 1.0
    monkeypatch.setenv("PIPELOOM_TEST_PERF_BUDGET_SCALE", "2.5")
    assert scale() == 2.5


def test_thousand_model_project_stays_inside_the_budget(tmp_path: Path) -> None:
    root = generate(tmp_path)
    opts = ProjectOptions(project_dir=root, env={})
    t: dict[str, float] = {}
    found = timed(
        "discovery_cold", t, lambda: Discovery(root, ["models"], use_cache=False).discover()
    )
    assert not found.failures and len(found.models) == N
    project = Project(opts)
    assert timed("resolve", t, project.resolve).value is not None
    assert timed("validate", t, project.validate).value is not None
    plan = timed("compile", t, project.compile).value
    assert plan is not None and len(plan.nodes) == N
    timed("compile_stable", t, lambda: manifest_bytes(plan, stable=True))
    # a second process finds the discovery cache the first one wrote
    timed("compile_warm", t, lambda: Project(opts).compile())
    print("perf seconds:", {k: round(v, 3) for k, v in t.items()})
    over = {k: (round(t[k], 2), b * scale()) for k, b in BUDGETS.items() if t[k] > b * scale()}
    assert not over, f"over budget (seconds, limit): {over}"
