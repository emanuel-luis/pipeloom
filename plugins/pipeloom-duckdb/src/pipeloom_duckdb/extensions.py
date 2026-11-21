"""The extension policy (ADR-0051): load from the pinned `extension_directory`, install from the
network only when `install_extensions` is on, and report PLX121 otherwise."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Final

from pipeloom.diagnostics import Diagnostic, Report

CODE: Final = "PLX121"


class ExtensionMissingError(RuntimeError):
    """A DuckDB extension is not installed and installing it is off. Permanent: a retry finds the
    same directory."""

    def __init__(self, diagnostic: Diagnostic) -> None:
        super().__init__(f"{diagnostic.code} {diagnostic.message} (hint: {diagnostic.hint})")
        self.diagnostic = diagnostic


def missing(name: str, directory: str | None) -> Diagnostic:
    """The PLX121 diagnostic for extension `name`; `directory` is the configured one."""
    where = (
        f"extension_directory '{directory}'"
        if directory
        else "DuckDB's default extension directory"
    )
    return Report().add(
        CODE,
        f"DuckDB extension '{name}' is not installed in {where}, and install_extensions is off",
        hint=(
            "preinstall it into that directory on a machine with network access (a run with "
            "install_extensions: true does it), or copy a preinstalled directory there"
        ),
        context={"extension": name, "extension_directory": directory},
    )


def plx121(project_dir: Path) -> Sequence[Diagnostic]:
    """The case-file scenario of PLX121 (tests/cases/PLX121.yml)."""
    return [missing("delta", "duckdb_extensions")]
