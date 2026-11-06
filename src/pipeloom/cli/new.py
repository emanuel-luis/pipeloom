"""The new command group: add files to an existing project."""

from __future__ import annotations

import os
import re
from pathlib import Path, PurePosixPath
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import log_format_option, project_options
from pipeloom.config.load import LoadedProject
from pipeloom.config.schema import SCHEMA_BASE_URL
from pipeloom.config.types import IDENTIFIER_PATTERN

_IDENT = re.compile(IDENTIFIER_PATTERN)

_INPUTS_EXAMPLE = """\
    # Declare inputs as aliases of other models (ref) or sources (source):
    # inputs:
    #   orders:
    #     ref: orders
"""

_STUB = '''\
"""Model {name}."""

from typing import Any


def build(ctx: Any, **inputs: Any) -> Any:
    """Build the model.

    Each input alias declared in {yml} arrives as a keyword argument.
    """
    raise NotImplementedError("write the model body in {py}")
'''


def _default_kind(loaded: LoadedProject) -> str:
    """python when the default target runs a pandas engine (no SQL there), else sql."""
    config = loaded.config
    target = config.default_target or (
        next(iter(config.targets)) if len(config.targets) == 1 else None
    )
    # default_target is checked only when the target is bound, so it may name no target here.
    found = config.targets.get(target) if target else None
    block = (found.engine if found else None) or config.engine
    return "python" if block is not None and block.kind == "pandas" else "sql"


def _model_yaml(name: str, python_ref: str | None) -> str:
    body = f"    python: {python_ref}\n" if python_ref else "    sql: |\n      select 1 as id\n"
    return (
        f"# yaml-language-server: $schema={SCHEMA_BASE_URL}models.json\n"
        f"models:\n  - name: {name}\n{body}{_INPUTS_EXAMPLE}"
    )


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


@click.group()
def new() -> None:
    """Add files to an existing project."""


@new.command("model")
@click.argument("name")
@project_options
@log_format_option
@click.option("--sql", "sql_", is_flag=True, help="Write a SQL model (default unless pandas).")
@click.option(
    "--python",
    "python_",
    is_flag=True,
    help="Write a Python model and its build() stub (default on a pandas default target).",
)
@click.option(
    "--folder",
    default=None,
    help="Folder under the first model path to put the file in. Default: the model path itself.",
)
def model(
    name: str,
    project_dir: Path,
    vars_: dict[str, Any],
    log_format: str,
    sql_: bool,
    python_: bool,
    folder: str | None,
) -> None:
    """Create a model named NAME: its YAML entry and, for Python, a build() stub.

    The paths written are printed, one per line.
    """
    if sql_ and python_:
        raise click.UsageError("--sql and --python cannot be used together")
    if not _IDENT.match(name):
        raise click.UsageError(
            f"'{name}' is not a valid model name: use letters, digits and underscores, "
            "not starting with a digit"
        )
    inv = Invocation("new model", log_format)
    opts = ProjectOptions(project_dir=project_dir, vars=vars_, env=os.environ)
    outcome = Project(opts, invocation_id=inv.invocation_id).load()
    loaded = outcome.value
    if loaded is None:
        inv.finish(outcome.diagnostics, 1)
        return
    config = loaded.config
    if name in {m.name for f in loaded.model_files for m in f.config.models}:
        raise click.UsageError(f"a model named '{name}' already exists")
    root = project_dir / config.model_paths[0]
    sub = PurePosixPath(folder or ".")
    yml = root.joinpath(*sub.parts) / f"{name}.yml"
    # Backslashes and drive letters would let Windows climb out of the folder; the resolved
    # check also catches symlinks.
    if (
        any(c in str(sub) for c in "\\:")
        or sub.is_absolute()
        or ".." in sub.parts
        or not yml.resolve().is_relative_to(root.resolve())
    ):
        raise click.UsageError(f"--folder '{folder}' must stay inside {config.model_paths[0]}")
    if yml.exists():
        raise click.UsageError(f"{yml.relative_to(project_dir).as_posix()} already exists")
    kind = "python" if python_ else "sql" if sql_ else _default_kind(loaded)
    written: list[Path] = []
    python_ref = None
    if kind == "python":
        # An explicit python_paths entry says where importable code lives; the default "." is not
        # a choice the project made.
        explicit = loaded.project_file.doc.data.get("python_paths")
        if not explicit:
            raise click.UsageError(
                "python models need python_paths in pipeloom.yml; add 'python_paths: [src]'"
            )
        package = f"{config.name}_models"
        pkg_dir = project_dir / config.python_paths[0] / package
        stub = pkg_dir / f"{name}.py"
        if stub.exists():
            raise click.UsageError(f"{stub.relative_to(project_dir).as_posix()} already exists")
        init_file = pkg_dir / "__init__.py"
        if not init_file.exists():
            _write(init_file, f'"""Python models of {config.name}."""\n')
            written.append(init_file)
        python_ref = f"{package}.{name}:build"
        rel_yml = yml.relative_to(project_dir).as_posix()
        _write(
            stub, _STUB.format(name=name, yml=rel_yml, py=stub.relative_to(project_dir).as_posix())
        )
        written.append(stub)
    _write(yml, _model_yaml(name, python_ref))
    written.append(yml)
    for path in written:
        click.echo(path.relative_to(project_dir).as_posix())
