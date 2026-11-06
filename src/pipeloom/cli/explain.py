"""The explain command: print the catalog entry of a diagnostic code."""

from __future__ import annotations

import click

from pipeloom.diagnostics import suggest
from pipeloom.diagnostics.codes import CATALOG, docs_url


@click.command()
@click.argument("code")
def explain(code: str) -> None:
    """Print what a diagnostic CODE means, for example PLX101 (case-insensitive).

    Shows the title, layer, severity, status, the version the code appeared in, the explanation
    and the link to the error-code page. Reads no project.
    """
    key = code.strip().upper()
    entry = CATALOG.get(key)
    if entry is None:
        message = f"unknown code '{code}'"
        hint = suggest(key, CATALOG)
        raise click.UsageError(f"{message}; {hint}" if hint else message)
    click.echo(f"{entry.code}  {entry.title}")
    click.echo(f"Layer: {entry.layer.value}")
    click.echo(f"Severity: {entry.severity.value}")
    click.echo(f"Status: {entry.status}")
    click.echo(f"Since: {entry.since}")
    click.echo("")
    click.echo(entry.explanation)
    click.echo("")
    click.echo(f"Docs: {docs_url(entry.code)}")
