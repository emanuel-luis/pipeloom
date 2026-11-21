"""The engine conformance kit: `EngineContract` bound to a harness named on the command line.

Run it from any project that installs pipeloom, pytest and the engine under test::

    pytest -p pipeloom.testing.pytest_plugin --pipeloom-harness my_engine_tests:harness

The plugin points pytest at this module. Subclass `EngineContract` in your own test module
instead when you want to add cells of your own beside the contract.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from pipeloom.testing.contract.engine import EngineContract, EngineHarness


class TestEngineKit(EngineContract):
    @pytest.fixture
    def engine_factory(self, request: pytest.FixtureRequest) -> Callable[[Path], EngineHarness]:
        from pipeloom.testing.pytest_plugin import load_harness

        return load_harness(request.config)
