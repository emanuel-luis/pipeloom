"""Temporary diagnostic printer; the event sinks replace it later."""

from __future__ import annotations

from collections.abc import Sequence

import click

from pipeloom.diagnostics import Diagnostic, Severity, format_diagnostic


def print_diagnostics(diagnostics: Sequence[Diagnostic]) -> tuple[int, int]:
    """Print to stderr; return the error and warning counts."""
    for d in diagnostics:
        click.echo(format_diagnostic(d), err=True)
    errors = sum(1 for d in diagnostics if d.severity is Severity.ERROR)
    warnings = sum(1 for d in diagnostics if d.severity is Severity.WARNING)
    return errors, warnings
