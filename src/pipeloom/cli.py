"""Command line entry point."""

import click


@click.group()
@click.version_option(package_name="pipeloom", prog_name="pipeloom")
def main() -> None:
    """pipeloom: declarative, engine-agnostic data pipelines in YAML."""
