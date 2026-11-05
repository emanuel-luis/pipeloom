"""The error the Airflow plugin raises when a DAG cannot be built."""

from __future__ import annotations

from collections.abc import Iterable

from pipeloom.diagnostics import Diagnostic, format_diagnostic


class PipeloomAirflowError(Exception):
    """A configuration or plan problem. `str()` is the formatted diagnostics, one per line."""

    def __init__(self, diagnostics: Iterable[Diagnostic]) -> None:
        self.diagnostics: tuple[Diagnostic, ...] = tuple(diagnostics)
        super().__init__("\n".join(format_diagnostic(d) for d in self.diagnostics))
