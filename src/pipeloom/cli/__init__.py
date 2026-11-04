"""Command line entry point."""

import click

from pipeloom.cli._options import PipeloomGroup
from pipeloom.cli.compile import compile_
from pipeloom.cli.graph import graph
from pipeloom.cli.ls import ls
from pipeloom.cli.plugins import plugins
from pipeloom.cli.run import run
from pipeloom.cli.schema import schema
from pipeloom.cli.validate import validate


@click.group(cls=PipeloomGroup)
@click.version_option(package_name="pipeloom", prog_name="pipeloom")
def main() -> None:
    """pipeloom: declarative, engine-agnostic data pipelines in YAML."""


main.add_command(validate)
main.add_command(compile_, name="compile")
main.add_command(ls)
main.add_command(run)
main.add_command(graph)
main.add_command(schema)
main.add_command(plugins)

__all__ = ["main"]
