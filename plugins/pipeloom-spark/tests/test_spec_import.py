"""The spec module is readable without importing a Spark library."""

from __future__ import annotations

import subprocess
import sys

CODE = """
import sys
import pipeloom_spark.spec as spec
assert spec.ENGINE_SPEC.capabilities.portable
assert spec.DELTA_PATH_SPEC.capabilities.views is False
assert spec.SPARK_CATALOG_SPEC.capabilities.views is True
for name in ("pyspark", "delta", "py4j", "pipeloom_spark.engine", "pipeloom_spark.session"):
    assert name not in sys.modules, name + " was imported"
print("ok")
"""

PURE = """
import sys
import pipeloom_spark.sqltext, pipeloom_spark.types, pipeloom_spark.errors
import pipeloom_spark.catalog, pipeloom_spark.engine, pipeloom_spark.io
assert "pyspark" not in sys.modules and "delta" not in sys.modules
print("ok")
"""


def _run(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, check=False
    )


def test_spec_import_does_not_import_spark() -> None:
    proc = _run(CODE)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"


def test_the_engine_module_loads_without_pyspark() -> None:
    proc = _run(PURE)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"
