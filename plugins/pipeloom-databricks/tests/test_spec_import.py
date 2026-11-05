"""The spec module is readable without importing a Databricks or Spark library."""

from __future__ import annotations

import subprocess
import sys

CODE = """
import sys
import pipeloom_databricks.spec as spec
assert spec.RENDERER_SPEC.group == "renderers" and spec.RENDERER_SPEC.kind == "databricks"
assert spec.UNITY_SPEC.group == "catalogs" and spec.SECRETS_SPEC.group == "secret_providers"
for name in ("databricks", "databricks.sdk", "pyspark", "pipeloom_spark", "pipeloom_databricks.render"):
    assert name not in sys.modules, name + " was imported"
print("ok")
"""

RENDER = """
import sys
import pipeloom_databricks.render, pipeloom_databricks.serialize
for name in ("databricks.sdk", "pyspark", "pipeloom_spark"):
    assert name not in sys.modules, name + " was imported"
print("ok")
"""

ENTRY = """
import importlib.metadata as md
found = {e.name: e.value for e in md.entry_points(group="pipeloom.renderers")}
assert found["databricks"] == "pipeloom_databricks.spec:RENDERER_SPEC", found
for group, name, attr in (
    ("catalogs", "unity", "UNITY_SPEC"),
    ("secret_providers", "databricks", "SECRETS_SPEC"),
):
    found = {e.name: e.value for e in md.entry_points(group="pipeloom." + group)}
    assert found[name] == "pipeloom_databricks.spec:" + attr, found
print("ok")
"""


def _run(code: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, check=False
    )


def test_spec_import_does_not_import_the_sdk_or_spark() -> None:
    proc = _run(CODE)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"


def test_the_renderer_modules_do_not_import_the_sdk_or_spark() -> None:
    proc = _run(RENDER)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == "ok"


def test_the_entry_point_is_registered() -> None:
    proc = _run(ENTRY)
    assert proc.returncode == 0, proc.stderr
