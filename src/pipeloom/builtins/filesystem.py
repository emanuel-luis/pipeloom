"""Filesystem catalog. Spec and options only; the implementation arrives in 0.2."""

from __future__ import annotations

from typing import Any

from pipeloom.engine.protocols import Catalog
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec


class FilesystemOptions(OptionsModel):
    root: str = "."
    """Directory under which schemas and tables are laid out."""


def _build(_opts: Any) -> Catalog:
    raise NotImplementedError("the filesystem catalog is implemented in pipeloom 0.2")


SPEC: PluginSpec[Catalog] = PluginSpec(
    "filesystem",
    "filesystem",
    "catalogs",
    API_VERSION,
    _build,
    FilesystemOptions,
    description="Tables as files under a directory (implementation in 0.2).",
)
