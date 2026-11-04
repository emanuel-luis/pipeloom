"""Coverage must measure directories, not package names.

For a package name, coverage.py imports the package inside `sys_modules_saved()` whenever
`sys.path` changes and then deletes every module imported meanwhile. `CallableLoader` changes
`sys.path` once per run, so on a threaded run a worker's import in flight (`m`, narwhals) lost its
`sys.modules` entry: `KeyError: 'm'` in `importlib._bootstrap._load_unlocked` (CI only).
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_coverage_sources_are_existing_directories() -> None:
    cfg = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    sources = cfg["tool"]["coverage"]["run"]["source"]
    assert sources, "coverage `source` must be set so CI can run a bare `--cov`"
    assert [s for s in sources if not (ROOT / s).is_dir()] == []


def test_ci_does_not_pass_package_names_to_cov() -> None:
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert re.findall(r"--cov=\S+", ci) == []
