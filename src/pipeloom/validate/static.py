"""L6: Python callables checked without importing them."""

from __future__ import annotations

import ast
import posixpath
from collections.abc import Sequence
from pathlib import Path

from pipeloom.diagnostics import Report
from pipeloom.graph.dag import ProjectGraph
from pipeloom.validate.semantics import where

_Def = ast.FunctionDef | ast.AsyncFunctionDef


def find_module(project_dir: Path, python_paths: Sequence[str], module: str) -> str | None:
    """The POSIX path (relative to the project) of `a.b`: `p/a/b.py`, then `p/a/b/__init__.py`."""
    parts = module.split(".")
    if not all(parts):
        return None
    for root in python_paths:
        base = posixpath.normpath(posixpath.join(root, *parts))
        for candidate in (f"{base}.py", f"{base}/__init__.py"):
            if (project_dir / candidate).is_file():
                return candidate
    return None


def _targets(node: ast.expr) -> set[str]:
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Tuple | ast.List):
        return set().union(*(_targets(e) for e in node.elts))
    if isinstance(node, ast.Starred):
        return _targets(node.value)
    return set()


def _bound_names(stmt: ast.stmt) -> set[str]:
    if isinstance(stmt, ast.ClassDef):
        return {stmt.name}
    if isinstance(stmt, ast.Assign):
        return set().union(*(_targets(t) for t in stmt.targets))
    if isinstance(stmt, ast.AnnAssign | ast.AugAssign):
        return _targets(stmt.target)
    if isinstance(stmt, ast.Import):
        return {a.asname or a.name.split(".")[0] for a in stmt.names}
    if isinstance(stmt, ast.ImportFrom):
        return {a.asname or a.name for a in stmt.names}
    return set()


def _mismatch(fn: _Def, aliases: set[str]) -> str | None:
    """Why the parameters do not fit `ctx` plus the aliases, or None.

    Without **kwargs the names must equal the aliases. With it any alias is accepted, and only
    a parameter without a default that is not an alias is an error.
    """
    a = fn.args
    positional = [*a.posonlyargs, *a.args]
    if not positional or positional[0].arg != "ctx":
        return "the first parameter must be named ctx"
    names = {p.arg for p in [*positional[1:], *a.kwonlyargs]}
    defaults = {p.arg for p in positional[len(positional) - len(a.defaults) :]}
    defaults |= {p.arg for p, d in zip(a.kwonlyargs, a.kw_defaults, strict=True) if d is not None}
    if a.kwarg is None:
        extra, missing = names - aliases, aliases - names
    else:
        extra, missing = (names - aliases) - defaults, set()
    bits = []
    if missing:
        bits.append(f"missing {sorted(missing)}")
    if extra:
        bits.append(f"unexpected {sorted(extra)}")
    return ", ".join(bits) or None


def check_static(pg: ProjectGraph, report: Report) -> None:
    """Report PLK601, PLK602 and PLK604 for Python models."""
    project = pg.project
    loaded = project.bound.loaded
    trees: dict[str, ast.Module | str] = {}
    for m in project.models.values():
        ref = m.entry.python
        if ref is None:
            continue
        module, _, name = ref.partition(":")
        loc = where(project, m, "python")
        ctx = {"model": m.name, "callable": ref}
        file = find_module(loaded.project_dir, loaded.config.python_paths, module)
        if file is None:
            path = module.replace(".", "/")
            report.add(
                "PLK601",
                f"module '{module}' of model '{m.name}' not found under python_paths",
                location=loc,
                hint=f"looked for {path}.py and {path}/__init__.py under "
                + ", ".join(loaded.config.python_paths),
                context=ctx,
            )
            continue
        if file not in trees:
            try:
                trees[file] = ast.parse((loaded.project_dir / file).read_bytes(), filename=file)
            except (SyntaxError, ValueError) as exc:
                trees[file] = f"{type(exc).__name__}: {exc}"
        tree = trees[file]
        if isinstance(tree, str):
            report.add("PLK601", f"cannot parse {file}: {tree}", location=loc, context=ctx)
            continue
        last: ast.stmt | None = None
        for stmt in tree.body:
            if (isinstance(stmt, _Def) and stmt.name == name) or name in _bound_names(stmt):
                last = stmt
        if last is None:
            report.add(
                "PLK601",
                f"function '{name}' is not defined at the top level of {file}",
                location=loc,
                context=ctx,
            )
        elif not isinstance(last, _Def):
            report.add(
                "PLK604",
                f"'{name}' in {file} is bound by something other than a def; "
                "its parameters were not checked",
                location=loc,
                context=ctx,
            )
        else:
            why = _mismatch(last, set(m.inputs))
            if why is not None:
                expected = ", ".join(["ctx", *m.inputs])
                report.add(
                    "PLK602",
                    f"parameters of '{ref}' do not match the inputs of '{m.name}': {why}",
                    location=loc,
                    hint=f"expected ({expected})",
                    context=ctx,
                )
