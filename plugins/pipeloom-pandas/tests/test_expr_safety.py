"""An `expr` from a config file must not reach Python: no `@name`, calls or attributes."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from pipeloom.api import Project, ProjectOptions
from pipeloom.events.bus import EventBus
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from pipeloom_pandas.engine import eval_series

ROOT = Path(__file__).resolve().parents[3] / "tests"
FRAME = pd.DataFrame({"id": [1, 2, 3], "email": ["a@x.com", "b@x.com", "c@y.com"]})


@pytest.mark.parametrize(
    "expr",
    [
        "@json.dumps(1) == '1'",
        "id > @Path.cwd().stat().st_size * 0",
        "id.abs() > 1",
        "email.str.contains('x')",
        "id.real > 1",
        "__import__('os').getcwd() == ''",
        "id > 1 if id else 2",
        "[i for i in id]",
    ],
)
def test_unsafe_expressions_are_rejected(expr: str) -> None:
    with pytest.raises((ValueError, TypeError, SyntaxError)):
        eval_series(FRAME, expr)


def test_plain_expressions_work() -> None:
    assert eval_series(FRAME, "id > 1").tolist() == [False, True, True]
    assert eval_series(FRAME, "id > 1 and email == 'b@x.com'").tolist() == [False, True, False]
    assert eval_series(FRAME, "id in (1, 3)").tolist() == [True, False, True]
    assert eval_series(FRAME, "id * 2 >= 4").tolist() == [False, True, True]


def test_an_unsafe_expr_in_a_model_is_plq302(tmp_path: Path) -> None:
    shutil.copytree(
        ROOT / "fixtures" / "quality",
        tmp_path,
        ignore=shutil.ignore_patterns("target", ".pipeloom"),
        dirs_exist_ok=True,
    )
    models = tmp_path / "models" / "models.yml"
    text = models.read_text(encoding="utf-8").replace("amount < 100", "@Path.cwd() == 1")
    models.write_text(text, encoding="utf-8", newline="\n")
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC), step=timedelta(0))
    proj = Project(
        ProjectOptions(project_dir=tmp_path, target="pandas", env={"PIPELOOM_USER": "t"}),
        clock=clock,
        invocation_id="inv",
    )
    with EventBus([], clock=clock, invocation_id="inv") as bus:
        proj.run(RunOptions(run_id="r1", workers=1), bus)
    doc = json.loads((tmp_path / "target" / "runs" / "r1" / "run_results.json").read_bytes())
    orders = next(r for r in doc["results"] if r["unit_id"] == "orders")
    assert orders["status"] == "failed" and orders["error"]["code"] == "PLQ302"
    assert not (tmp_path / "target" / "warehouse" / "main" / "orders").exists()
