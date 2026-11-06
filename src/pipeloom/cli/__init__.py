"""Command line entry point."""

import click

from pipeloom.cli._ext import PipeloomGroup
from pipeloom.cli.compile import compile_
from pipeloom.cli.docs import docs
from pipeloom.cli.explain import explain
from pipeloom.cli.finalize import finalize
from pipeloom.cli.graph import graph
from pipeloom.cli.init import init
from pipeloom.cli.ls import ls
from pipeloom.cli.new import new
from pipeloom.cli.plugins import plugins
from pipeloom.cli.retry import retry
from pipeloom.cli.run import run
from pipeloom.cli.schema import schema
from pipeloom.cli.validate import validate


@click.group(cls=PipeloomGroup)
@click.version_option(package_name="pipeloom", prog_name="pipeloom")
def main() -> None:
    """pipeloom: declarative, engine-agnostic data pipelines in YAML."""


main.add_command(init)
main.add_command(new)
main.add_command(validate)
main.add_command(compile_, name="compile")
main.add_command(ls)
main.add_command(run)
main.add_command(retry)
main.add_command(finalize)
main.add_command(graph)
main.add_command(docs)
main.add_command(schema)
main.add_command(explain)
main.add_command(plugins)

__all__ = ["main"]
