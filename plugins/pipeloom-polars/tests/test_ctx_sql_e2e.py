"""ctx.sql returns a Narwhals frame under frame_api: narwhals."""

from __future__ import annotations

from pathlib import Path

from tests.conftest import make_project

from pipeloom.api import Project, ProjectOptions
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock

FILES = {
    "pipeloom.yml": (
        "config_version: 1\nname: shop\npython_paths:\n  - src\ntargets:\n  dev:\n"
        "    engine:\n      kind: polars\n    catalog:\n      kind: filesystem\n"
        "      root: wh\n"
    ),
    "models/m.yml": (
        "models:\n  - name: a\n    python: 'm:a'\n"
        "  - name: b\n    python: 'm:b'\n    frame_api: narwhals\n"
        "    inputs:\n      a:\n        ref: a\n"
    ),
    "src/m.py": (
        "import narwhals as nw\n"
        "import polars as pl\n\n"
        "def a(ctx):\n    return pl.LazyFrame({'id': [1, 2, 3]})\n\n"
        "def b(ctx, a):\n"
        "    out = ctx.sql('select id from a where id > 1')\n"
        "    assert isinstance(out, nw.LazyFrame), type(out)\n"
        "    return out.with_columns(nw.col('id') * 10)\n"
    ),
}


def test_ctx_sql_under_narwhals(tmp_path: Path) -> None:
    make_project(tmp_path, FILES)
    project = Project(ProjectOptions(project_dir=tmp_path, env={}), clock=FakeClock())
    outcome = project.run(RunOptions(run_id="r", workers=1))
    assert outcome.exit_code == 0, [d.message for d in outcome.diagnostics]
