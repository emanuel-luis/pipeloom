"""Plugin specs and entry-point doubles used by the plugin tests and case files."""

from __future__ import annotations

import importlib.metadata
from dataclasses import dataclass
from typing import Any

import pytest

from pipeloom.engine.types import EngineCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec


class FakeOptions(OptionsModel):
    path: str = "x"
    """Where to put it."""
    size: int = 1


def _build(_opts: Any) -> object:
    return object()


API03: PluginSpec[object] = PluginSpec("future", "future", "engines", (0, 3), _build, FakeOptions)
MEMORY_CLONE: PluginSpec[object] = PluginSpec(
    "memory", "memory", "engines", API_VERSION, _build, FakeOptions
)
FAKE_ENGINE: PluginSpec[object] = PluginSpec(
    "fake", "fake", "engines", API_VERSION, _build, FakeOptions, description="A fake engine."
)
NOSQL: PluginSpec[object] = PluginSpec(
    "nosql",
    "nosql",
    "engines",
    API_VERSION,
    _build,
    FakeOptions,
    capabilities=EngineCapabilities(
        formats=frozenset({"memory"}),
        strategies=frozenset({"append", "overwrite"}),
        materializations=frozenset({"table", "ephemeral"}),
    ),
    description="An engine with no SQL, views, idempotent writes or portable frames.",
)
FAKE_SINK: PluginSpec[object] = PluginSpec(
    "tap", "tap", "sinks", API_VERSION, _build, FakeOptions, description="A fake sink."
)
DUP_A: PluginSpec[object] = PluginSpec("dup", "dup", "engines", API_VERSION, _build, FakeOptions)
DUP_B: PluginSpec[object] = PluginSpec("dup", "dup", "engines", API_VERSION, _build, FakeOptions)
WRONG_GROUP: PluginSpec[object] = PluginSpec("x", "x", "catalogs", API_VERSION, _build, FakeOptions)
NOT_A_SPEC = 42


@dataclass(frozen=True)
class FakeDist:
    name: str
    version: str


@dataclass(frozen=True)
class FakeEntryPoint:
    name: str
    value: str
    group: str
    dist: FakeDist | None = None


def install_entry_points(
    monkeypatch: pytest.MonkeyPatch, points: list[FakeEntryPoint]
) -> list[str]:
    """Replace importlib.metadata.entry_points; returns the groups that were asked for."""
    asked: list[str] = []

    def entry_points(*, group: str) -> list[FakeEntryPoint]:
        asked.append(group)
        return [p for p in points if f"pipeloom.{p.group}" == group]

    monkeypatch.setattr(importlib.metadata, "entry_points", entry_points)
    return asked


class StrictOptions(OptionsModel):
    url: str
    """Where to connect."""
    retries: int = 0


STRICT: PluginSpec[object] = PluginSpec(
    "strict", "strict", "engines", API_VERSION, _build, StrictOptions
)
