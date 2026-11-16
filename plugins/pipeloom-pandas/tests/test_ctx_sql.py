"""pandas has no SQL: ctx.sql warns in validation (PLK606) and fails the unit (PLX101)."""

from __future__ import annotations

from pathlib import Path

from tests.conftest import make_project

from pipeloom.api import Project, ProjectOptions
from pipeloom.runtime.run import RunOptions
from pipeloom.testing.clock import FakeClock
from pipeloom_pandas.spec import CAPABILITIES

FILES = {
    "pipeloom.yml": (
        "config_version: 1\nname: shop\npython_paths:\n  - src\ntargets:\n  dev:\n"
        "    engine:\n      kind: pandas\n    catalog:\n      kind: filesystem\n"
        "      root: wh\n"
    ),
    "models/m.yml": "models:\n  - name: a\n    python: 'm:a'\n",
    "src/m.py": "def a(ctx):\n    return ctx.sql('select 1 as id')\n",
}


def test_pandas_declares_no_sql_dialect() -> None:
    assert CAPABILITIES.sql_dialect is None


def test_ctx_sql_on_pandas(tmp_path: Path) -> None:
    make_project(tmp_path, FILES)
    project = Project(ProjectOptions(project_dir=tmp_path, env={}), clock=FakeClock())
    found = {d.code: d for d in project.run(RunOptions(run_id="r", workers=1)).diagnostics}
    assert found["PLK606"].severity.value == "warning"
    error = found["PLX101"]
    assert "has no SQL" in error.message
    assert error.hint == "write the model with frames, or choose an engine with SQL"
