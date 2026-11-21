"""The conformance kit runs from outside the repo's pytest config, through the opt-in plugin."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from pipeloom.testing.contract import KIT_VERSION

ROOT = Path(__file__).parent.parent.parent


def kit(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    (tmp_path / "pytest.ini").write_text("[pytest]\n", encoding="utf-8")
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-p", "pipeloom.testing.pytest_plugin", *args],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_the_kit_runs_the_engine_suite_with_a_versioned_header(tmp_path: Path) -> None:
    proc = kit(
        tmp_path,
        "--pipeloom-harness",
        "tests.test_engine_contract:harness",
        "-k",
        "capabilities_shape or test_metrics",
        "unrelated_path_that_the_kit_replaces",
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert f"pipeloom conformance kit {KIT_VERSION} (pipeloom " in proc.stdout
    assert "harness tests.test_engine_contract:harness" in proc.stdout
    assert "2 passed" in proc.stdout


def test_an_unloadable_harness_is_a_usage_error(tmp_path: Path) -> None:
    proc = kit(tmp_path, "--pipeloom-harness", "no_such_module:harness")
    assert proc.returncode == 4  # pytest's usage error
    assert "cannot be loaded" in proc.stdout + proc.stderr


def test_the_plugin_stays_idle_without_the_option(tmp_path: Path) -> None:
    (tmp_path / "test_x.py").write_text("def test_x(): pass\n", encoding="utf-8")
    proc = kit(tmp_path)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "1 passed" in proc.stdout
    assert "harness" not in proc.stdout.split("collected")[0]


def test_the_bare_pytest_script_finds_a_harness_in_the_current_directory(tmp_path: Path) -> None:
    (tmp_path / "my_harness.py").write_text(
        "from tests.test_engine_contract import harness\n", encoding="utf-8"
    )
    exe = Path(sys.executable).parent / ("pytest.exe" if os.name == "nt" else "pytest")
    # The repo root stays on PYTHONPATH only for `tests.test_engine_contract`, not for my_harness.
    env = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
    proc = subprocess.run(
        [str(exe), "-p", "pipeloom.testing.pytest_plugin", "--pipeloom-harness",
         "my_harness:harness", "-k", "capabilities_shape"],  # fmt: skip
        cwd=tmp_path, env=env, capture_output=True, text=True, check=False, timeout=120,
    )  # fmt: skip
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "1 passed" in proc.stdout
