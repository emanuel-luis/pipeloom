"""The spec module is readable without importing pandas or pyarrow."""

from __future__ import annotations

import subprocess
import sys

CODE = """
import sys
import pipeloom_pandas.spec as spec
assert spec.SPEC.capabilities.portable
assert not spec.SPEC.capabilities.sql_models
assert "pandas" not in sys.modules, "pandas was imported"
assert "pyarrow" not in sys.modules, "pyarrow was imported"
assert "pipeloom_pandas.engine" not in sys.modules
print("ok")
"""


def test_spec_import_does_not_import_pandas() -> None:
    proc = subprocess.run(
        [sys.executable, "-I", "-c", CODE], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"
