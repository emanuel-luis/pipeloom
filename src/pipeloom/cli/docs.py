"""The docs commands: render the plan and the last run as one HTML file, and serve it."""

from __future__ import annotations

import json
import os
from pathlib import Path, PurePosixPath
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.artifacts.results import RESULTS_SCHEMA
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import (
    log_format_option,
    plugin_option,
    project_options,
    target_option,
)
from pipeloom.diagnostics import Report, Severity
from pipeloom.docs.render import render_site
from pipeloom.docs.serve import make_server
from pipeloom.graph.selectors import Selection
from pipeloom.plan.manifest import read_manifest
from pipeloom.plan.model import Plan
from pipeloom.runtime.run import artifacts_root

DEFAULT_DIR = "target/docs"


def _load_results(path: Path, project: str) -> dict[str, Any]:
    """The run results document, or a usage error naming the file."""
    try:
        doc = json.loads(path.read_bytes().decode("utf-8"))
    except (OSError, ValueError):
        doc = None
    if not isinstance(doc, dict) or doc.get("schema") != RESULTS_SCHEMA:
        raise click.UsageError(f"{path.as_posix()} is not a {RESULTS_SCHEMA} document")
    run = doc.get("run")
    if (
        not isinstance(run, dict)
        or not isinstance(doc.get("results"), list)
        or run.get("project") != project
    ):
        raise click.UsageError(
            f"{path.as_posix()} does not belong to project '{project}' or has no run"
        )
    return doc


@click.group()
def docs() -> None:
    """Build and serve the docs site (one HTML file, read only)."""


@docs.command()
@project_options
@target_option
@plugin_option
@log_format_option
@click.option(
    "--out",
    type=click.Path(path_type=Path),
    default=None,
    help=f"Output folder, relative to the project dir unless absolute. Default: {DEFAULT_DIR}.",
)
@click.option(
    "--manifest",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="Render this manifest instead of compiling the project.",
)
@click.option(
    "--run-results",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=None,
    help="A run_results.json to show. Default: the one at the root of the artifacts root, "
    "when it exists (never with --manifest).",
)
def generate(
    project_dir: Path,
    vars_: dict[str, Any],
    target: str | None,
    plugins: tuple[str, ...],
    log_format: str,
    out: Path | None,
    manifest: Path | None,
    run_results: Path | None,
) -> None:
    """Write index.html: the graph, every model and source, and the last run's results."""
    inv = Invocation("docs generate", log_format)
    results_path = run_results
    plan: Plan | None
    if manifest is not None:
        report = Report()
        plan = read_manifest(manifest, report)
        diags = report.sorted()
        bound = None
        if plan is None:
            inv.finish(diags, 1)
            return
    else:
        opts = ProjectOptions(
            project_dir=project_dir,
            target=target,
            vars=vars_,
            boundary=None,
            plugins=plugins,
            env=os.environ,
        )
        project = Project(opts, invocation_id=inv.invocation_id)
        outcome = project.compile(Selection())
        bound = project.bind().value
        diags = outcome.diagnostics
        plan = outcome.value
        if plan is None or bound is None:
            inv.finish(diags, 1, bound=bound)
            return
        if results_path is None:
            found = artifacts_root(bound, project_dir) / "run_results.json"
            results_path = found if found.is_file() else None
    results = _load_results(results_path, plan.metadata.project) if results_path else None
    chosen = out if out is not None else Path(DEFAULT_DIR)
    folder = chosen if chosen.is_absolute() else project_dir / chosen
    folder.mkdir(parents=True, exist_ok=True)
    content = render_site(plan, results).encode("utf-8")
    page = folder / "index.html"
    page.write_bytes(content)
    # like compile: the event path is relative to the project dir unless --out is absolute
    reported = page if chosen.is_absolute() else chosen / "index.html"
    inv.artifact(PurePosixPath(reported.as_posix()), content, "docs")
    click.echo(page.as_posix())
    inv.finish([d for d in diags if d.severity is not Severity.ERROR], 0, bound=bound)


@docs.command()
@click.option(
    "--project-dir",
    envvar="PIPELOOM_PROJECT_DIR",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    default=".",
    show_default=True,
    help="Directory that holds pipeloom.yml.",
)
@click.option(
    "--dir",
    "directory",
    type=click.Path(path_type=Path),
    default=None,
    help=f"Folder to serve, relative to the project dir unless absolute. Default: {DEFAULT_DIR}.",
)
@click.option("--host", default="127.0.0.1", show_default=True, help="Address to listen on.")
@click.option(
    "--port", type=click.IntRange(0, 65535), default=8000, show_default=True, help="Port to use."
)
def serve(project_dir: Path, directory: Path | None, host: str, port: int) -> None:
    """Serve the generated site until Ctrl+C. It never opens a browser."""
    folder = directory if directory is not None else Path(DEFAULT_DIR)
    folder = folder if folder.is_absolute() else project_dir / folder
    if not (folder / "index.html").is_file():
        raise click.UsageError(
            f"{folder.as_posix()}/index.html not found; run `pipeloom docs generate` first"
        )
    try:
        server = make_server(folder, host, port)
    except OSError as err:
        raise click.UsageError(f"cannot listen on {host}:{port}: {err}") from err
    click.echo(f"serving {folder.as_posix()} at http://{host}:{server.server_address[1]}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
