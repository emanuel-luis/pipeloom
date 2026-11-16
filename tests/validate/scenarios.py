"""Case-file scenarios (`python: tests.validate.scenarios:<name>`) that need a patched check."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from unittest import mock

from pipeloom.api import Project, ProjectOptions
from pipeloom.diagnostics import Diagnostic


def plc513(project_dir: Path) -> Sequence[Diagnostic]:
    """Validate with sqlglot reported missing, as on an install without the extra."""
    with mock.patch("pipeloom.validate.capabilities.sqlglot_installed", return_value=False):
        return Project(ProjectOptions(project_dir=project_dir, env={})).validate().diagnostics
