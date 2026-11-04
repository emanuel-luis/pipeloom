"""Command line entry point."""

import click

from pipeloom.cli._options import PipeloomGroup
from pipeloom.cli.validate import validate


@click.group(cls=PipeloomGroup)
@click.version_option(package_name="pipeloom", prog_name="pipeloom")
def main() -> None:
    """pipeloom: declarative, engine-agnostic data pipelines in YAML."""


main.add_command(validate)

__all__ = ["main"]
