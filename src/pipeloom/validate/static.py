"""L6: Python callables checked without importing them."""

from __future__ import annotations

import ast
import importlib.util
import posixpath
from collections.abc import Sequence
from pathlib import Path

from pipeloom.config.resolve import ResolvedModel
from pipeloom.diagnostics import Location, Report
from pipeloom.engine.types import EngineCapabilities
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


def model_file(project_dir: Path, python_paths: Sequence[str], m: ResolvedModel) -> str | None:
    """The file of a Python model: the decorator's file (which may lie in a `model_packages`
    package outside `python_paths`), else the module found under `python_paths`."""
    if m.site is not None:
        return m.site.file
    return find_module(project_dir, python_paths, (m.entry.python or "").partition(":")[0])


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


def _calls_ctx_sql(fn: _Def) -> bool:
    return any(
        isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "sql"
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "ctx"
        for n in ast.walk(fn)
    )


def _literal_queries(project_dir: Path, m: ResolvedModel) -> list[tuple[str, set[str], Location]]:
    """The literal `ctx.sql("...")` queries of a decorated model's function, with the names its
    literal `inputs=` mapping adds and the call's location."""
    attr = (m.entry.python or "").partition(":")[2]
    try:
        tree = ast.parse((project_dir / m.file).read_bytes(), filename=m.file)
    except (OSError, SyntaxError, ValueError):
        return []
    fns = [s for s in tree.body if isinstance(s, _Def) and s.name == attr]
    out: list[tuple[str, set[str], Location]] = []
    for node in ast.walk(fns[-1]) if fns else ():
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "ctx"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            continue
        extra: set[str] = set()
        for kw in node.keywords:
            if kw.arg == "inputs" and isinstance(kw.value, ast.Dict):
                extra |= {
                    k.value
                    for k in kw.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
        loc = Location(m.file, node.args[0].lineno, node.args[0].col_offset + 1)
        out.append((node.args[0].value, extra, loc))
    return out


def check_sql(pg: ProjectGraph, report: Report) -> None:
    """Report PLK603 and PLK605 for SQL models. Does nothing unless sqlglot is installed."""
    if importlib.util.find_spec("sqlglot") is None:
        return
    from sqlglot import exp, parse
    from sqlglot.errors import SqlglotError

    project = pg.project
    queries: list[tuple[ResolvedModel, str, set[str], Location]] = []
    for m in project.models.values():
        if m.entry.sql is not None:
            queries.append((m, m.entry.sql, set(m.inputs), where(project, m, "sql")))
        elif m.site is not None:
            queries += [
                (m, sql, set(m.inputs) | extra, loc)
                for sql, extra, loc in _literal_queries(project.bound.loaded.project_dir, m)
            ]
    for m, sql, aliases, loc in queries:
        ctx = {"model": m.name}
        try:
            statements = parse(sql)
        except SqlglotError:
            report.add(
                "PLK605",
                f"SQL of model '{m.name}' cannot be parsed, so its tables were not checked",
                location=loc,
                hint="sqlglot parses it with its default dialect",
                context=ctx,
            )
            continue
        unknown: set[str] = set()
        for tree in statements:
            if tree is None:
                continue
            ctes = {c.alias for c in tree.find_all(exp.CTE)}
            for table in tree.find_all(exp.Table):
                name = ".".join(p for p in (table.catalog, table.db, table.name) if p)
                if table.name and name not in ctes and name not in aliases:
                    unknown.add(name)
        for name in sorted(unknown):
            report.add(
                "PLK603",
                f"SQL of model '{m.name}' reads '{name}', which is not an input alias",
                location=loc,
                hint="declare it under inputs, or ignore this warning for an external table",
                context={**ctx, "table": name},
            )


def check_static(pg: ProjectGraph, report: Report) -> None:
    """Report PLK601, PLK602, PLK604 and PLK606 for Python models."""
    project = pg.project
    loaded = project.bound.loaded
    engine = project.bound.plugins.get("engines")
    caps = None if engine is None else engine.spec.capabilities
    no_sql = (
        engine.kind
        if engine is not None and isinstance(caps, EngineCapabilities) and caps.sql_dialect is None
        else None
    )
    trees: dict[str, ast.Module | str] = {}
    for b in project.binding_problems:
        if b.code == "PLK602":
            report.add(b.code, b.message, location=b.location, hint=b.hint, context=dict(b.context))
    for m in project.models.values():
        ref = m.entry.python
        if ref is None:
            continue
        module, _, name = ref.partition(":")
        loc = where(project, m, "python")
        ctx = {"model": m.name, "callable": ref}
        file = model_file(loaded.project_dir, loaded.config.python_paths, m)
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
            if m.site is None:
                report.add("PLK601", f"cannot parse {file}: {tree}", location=loc, context=ctx)
            continue
        last: ast.stmt | None = None
        for stmt in tree.body:
            if (isinstance(stmt, _Def) and stmt.name == name) or name in _bound_names(stmt):
                last = stmt
        if m.site is not None and not isinstance(last, _Def):
            # Discovery imported the callable; loops and factories are allowed (ADR-0050 point 2).
            continue
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
            if no_sql is not None and _calls_ctx_sql(last):
                report.add(
                    "PLK606",
                    f"'{ref}' calls ctx.sql, but engine '{no_sql}' has no SQL",
                    location=loc,
                    hint="write the model with frames, or choose an engine with SQL",
                    context={**ctx, "engine": no_sql},
                )
            # A decorated model's parameters are its inputs; binding reported PLK602 already.
            why = _mismatch(last, set(m.inputs)) if m.site is None else None
            if why is not None:
                expected = ", ".join(["ctx", *m.inputs])
                report.add(
                    "PLK602",
                    f"parameters of '{ref}' do not match the inputs of '{m.name}': {why}",
                    location=loc,
                    hint=f"expected ({expected})",
                    context=ctx,
                )
