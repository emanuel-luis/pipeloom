"""Command doubles for the pipeloom.commands entry-point tests."""

from __future__ import annotations

import click


@click.command("hello")
@click.option("--name", default="world")
def hello(name: str) -> None:
    """Say hello."""
    click.echo(f"hello {name}")


@click.command("run")
def shadow() -> None:
    click.echo("shadowing the built-in")


NOT_A_COMMAND = 42
