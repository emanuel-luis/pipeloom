"""The new command group: add files to an existing project."""

from __future__ import annotations

import keyword
import os
import re
from pathlib import Path, PurePosixPath
from typing import Any

import click

from pipeloom.api import Project, ProjectOptions
from pipeloom.cli._events import Invocation
from pipeloom.cli._options import log_format_option, project_options
from pipeloom.config.load import LoadedProject
from pipeloom.config.types import IDENTIFIER_PATTERN
from pipeloom.discovery import discover

_IDENT = re.compile(IDENTIFIER_PATTERN)
# Names the module itself uses: a function with one of them would shadow the import or `ctx`.
_RESERVED = {"ctx", "model", "ModelContext"}

_SQL = '''\
"""Model {name}."""

from pipeloom import ModelContext, model


@model
def {name}(ctx: ModelContext):
    # Add one parameter per input, named like the model or source table it reads, and use that
    # name in the query.
    return ctx.sql("select 1 as id")
'''

_PYTHON = '''\
"""Model {name}."""

from pipeloom import ModelContext, model


@model
def {name}(ctx: ModelContext):
    # Add one parameter per input, named like the model or source table it reads. Each arrives
    # as a frame of the engine; return one too.
    raise NotImplementedError("write the body of {name} in {path}")
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


def _model_names(loaded: LoadedProject) -> set[str]:
    """Models declared in YAML and decorated functions, by name."""
    config = loaded.config
    names = {m.name for f in loaded.model_files for m in f.config.models}
    found = discover(
        loaded.project_dir, config.model_paths, config.model_packages, config.python_paths
    )
    return names | {m.name for m in found.models}


@click.group()
def new() -> None:
    """Add files to an existing project."""


@new.command("model")
@click.argument("name")
@project_options
@log_format_option
@click.option(
    "--sql",
    "sql_",
    is_flag=True,
    help="Write a body that returns ctx.sql(...) (default unless pandas).",
)
@click.option(
    "--python",
    "python_",
    is_flag=True,
    help="Write a frame stub (default on a pandas default target).",
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
    """Create a model named NAME: a decorated function in models/<folder>/NAME.py.

    The path written is printed.
    """
    if sql_ and python_:
        raise click.UsageError("--sql and --python cannot be used together")
    if not _IDENT.match(name):
        raise click.UsageError(
            f"'{name}' is not a valid model name: use letters, digits and underscores, "
            "not starting with a digit"
        )
    if keyword.iskeyword(name) or name in _RESERVED:
        raise click.UsageError(f"'{name}' cannot name a model function: pick another name")
    inv = Invocation("new model", log_format)
    opts = ProjectOptions(project_dir=project_dir, vars=vars_, env=os.environ)
    outcome = Project(opts, invocation_id=inv.invocation_id).load()
    loaded = outcome.value
    if loaded is None:
        inv.finish(outcome.diagnostics, 1)
        return
    config = loaded.config
    root = project_dir / config.model_paths[0]
    sub = PurePosixPath(folder or ".")
    py = root.joinpath(*sub.parts) / f"{name}.py"
    # Backslashes and drive letters would let Windows climb out of the folder; the resolved
    # check also catches symlinks.
    if (
        any(c in str(sub) for c in "\\:")
        or sub.is_absolute()
        or ".." in sub.parts
        or not py.resolve().is_relative_to(root.resolve())
    ):
        raise click.UsageError(f"--folder '{folder}' must stay inside {config.model_paths[0]}")
    rel = py.relative_to(project_dir).as_posix()
    if py.exists():
        raise click.UsageError(f"{rel} already exists")
    if name in _model_names(loaded):
        raise click.UsageError(f"a model named '{name}' already exists")
    kind = "python" if python_ else "sql" if sql_ else _default_kind(loaded)
    text = (_PYTHON if kind == "python" else _SQL).format(name=name, path=rel)
    py.parent.mkdir(parents=True, exist_ok=True)
    py.write_text(text, encoding="utf-8", newline="\n")
    click.echo(rel)
