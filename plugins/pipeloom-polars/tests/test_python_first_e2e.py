"""Decorated models on polars: a frame model and a ctx.sql model, run end to end (ADR-0050)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from pipeloom.api import Project, ProjectOptions
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock

FILES = {
    "pipeloom.yml": """\
config_version: 1
name: shop
python_paths: [models]
targets:
  dev:
    engine:
      kind: polars
    catalog:
      kind: filesystem
      root: wh
""",
    "models/_properties.yml": """\
models:
  - name: big_orders
    description: Orders above 10.
""",
    "models/orders_models.py": """\
import polars as pl

from pipeloom import model


@model
def orders(ctx):
    return pl.LazyFrame({"id": [1, 2, 3], "amount": [5.0, 12.0, 20.0]})


@model(tags=["sql"])
def big_orders(ctx, orders):
    return ctx.sql("select id, amount from orders where amount > 10")
""",
}


def test_decorated_models_run_on_polars(tmp_path: Path) -> None:
    for name, text in FILES.items():
        (tmp_path / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / name).write_text(text, encoding="utf-8")
    project = Project(ProjectOptions(project_dir=tmp_path, env={}), clock=FakeClock())
    outcome = project.run(RunOptions(run_id="r", workers=1))
    assert outcome.exit_code == 0, [d.message for d in outcome.diagnostics]
    written = sorted(
        p for p in (tmp_path / "wh").rglob("*.parquet") if "big_orders" in p.as_posix()
    )
    assert written, sorted(p.as_posix() for p in (tmp_path / "wh").rglob("*"))
    assert sorted(pl.read_parquet(written).get_column("id").to_list()) == [2, 3]
