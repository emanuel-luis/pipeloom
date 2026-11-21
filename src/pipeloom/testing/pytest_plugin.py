"""Opt-in pytest plugin that runs the engine conformance kit (`-p pipeloom.testing.pytest_plugin`).

Nothing registers it: pipeloom declares no `pytest11` entry point, so it never loads on its own.
"""

from __future__ import annotations

import importlib
import sys
from collections.abc import Callable
from importlib.metadata import version
from pathlib import Path

import pytest

from pipeloom.testing.contract import KIT_VERSION
from pipeloom.testing.contract.engine import EngineHarness

OPTION = "--pipeloom-harness"


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("pipeloom", "pipeloom conformance kit")
    group.addoption(
        OPTION,
        dest="pipeloom_harness",
        metavar="MODULE:CALLABLE",
        default=None,
        help="run the engine conformance kit; CALLABLE takes a tmp_path and returns an "
        "EngineHarness. The command-line paths are replaced by the kit.",
    )


def pytest_report_header(config: pytest.Config) -> str:
    line = f"pipeloom conformance kit {KIT_VERSION} (pipeloom {version('pipeloom')})"
    spec = config.getoption("pipeloom_harness")
    return f"{line}, harness {spec}" if spec else line


HARNESS = pytest.StashKey[Callable[[Path], EngineHarness]]()


def pytest_configure(config: pytest.Config) -> None:
    spec = config.getoption("pipeloom_harness")
    if not spec:
        return
    config.stash[HARNESS] = _resolve(config, spec)
    from pipeloom.testing import kit

    config.args = [str(Path(kit.__file__))]


def _resolve(config: pytest.Config, spec: str) -> Callable[[Path], EngineHarness]:
    """Import `MODULE:CALLABLE` from the invocation directory; a bad value is a usage error."""
    # The bare `pytest` script leaves the current directory off sys.path, unlike `python -m pytest`.
    here = str(config.invocation_params.dir)
    if here not in sys.path:
        sys.path.insert(0, here)
    module, _, attr = spec.partition(":")
    try:
        target = getattr(importlib.import_module(module), attr)
    except (ImportError, AttributeError) as exc:
        raise pytest.UsageError(f"{OPTION} {spec!r} cannot be loaded: {exc}") from exc
    if not callable(target):
        raise pytest.UsageError(f"{OPTION} {spec!r} is not callable")
    return target  # type: ignore[no-any-return]


def load_harness(config: pytest.Config) -> Callable[[Path], EngineHarness]:
    """The harness `pytest_configure` resolved."""
    try:
        return config.stash[HARNESS]
    except KeyError:
        raise pytest.UsageError(f"{OPTION} MODULE:CALLABLE is required to run the kit") from None
