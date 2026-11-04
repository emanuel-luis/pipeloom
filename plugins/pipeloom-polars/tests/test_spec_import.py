"""The spec module is readable without importing polars."""

from __future__ import annotations

import subprocess
import sys

CODE = """
import sys
import pipeloom_polars.spec as spec
assert spec.SPEC.capabilities.portable
assert "polars" not in sys.modules, "polars was imported"
assert "pipeloom_polars.engine" not in sys.modules
print("ok")
"""


def test_spec_import_does_not_import_polars() -> None:
    proc = subprocess.run(
        [sys.executable, "-I", "-c", CODE], capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"
