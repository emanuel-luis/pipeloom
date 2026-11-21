"""L6: Python callables checked without importing them."""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from typing import Any

from pipeloom.config.resolve import ResolvedModel
from pipeloom.diagnostics import Location, Report
from pipeloom.engine.types import EngineCapabilities
from pipeloom.graph.dag import ProjectGraph
from pipeloom.validate.semantics import where

_Def = ast.FunctionDef | ast.AsyncFunctionDef


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


def _calls_ctx_sql(fn: _Def) -> bool:
    return any(
        isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "sql"
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "ctx"
        for n in ast.walk(fn)
    )


def _parse(project_dir: Path, file: str) -> ast.Module | None:
    try:
        return ast.parse((project_dir / file).read_bytes(), filename=file)
    except (OSError, SyntaxError, ValueError):
        return None


def _literal_queries(
    tree: ast.Module | None, m: ResolvedModel
) -> list[tuple[str, set[str], Location, str | None]]:
    """The literal `ctx.sql("...")` queries of a decorated model's function in its module's
    `tree`, with the names its literal `inputs=` mapping adds, the call's location and its literal
    `dialect=`."""
    if tree is None:
        return []
    attr = m.entry.python.partition(":")[2]
    fns = [s for s in tree.body if isinstance(s, _Def) and s.name == attr]
    out: list[tuple[str, set[str], Location, str | None]] = []
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
        dialect: str | None = None
        for kw in node.keywords:
            if kw.arg == "inputs" and isinstance(kw.value, ast.Dict):
                extra |= {
                    k.value
                    for k in kw.value.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
            if (
                kw.arg == "dialect"
                and isinstance(kw.value, ast.Constant)
                and isinstance(kw.value.value, str)
            ):
                dialect = kw.value.value
        loc = Location(m.file, node.args[0].lineno, node.args[0].col_offset + 1)
        out.append((node.args[0].value, extra, loc, dialect))
    return out


def check_sql(pg: ProjectGraph, report: Report) -> None:
    """Report PLK603 and PLK605 for literal `ctx.sql` queries. Does nothing unless sqlglot is
    installed."""
    if importlib.util.find_spec("sqlglot") is None:
        return
    from sqlglot import exp, parse
    from sqlglot.errors import SqlglotError

    project = pg.project
    engine = project.bound.plugins.get("engines")
    caps = None if engine is None else engine.spec.capabilities
    engine_dialect = caps.sql_dialect if isinstance(caps, EngineCapabilities) else None
    queries: list[tuple[ResolvedModel, str, set[str], Location, str | None]] = []
    # one parse per module: a module can hold many models
    trees: dict[str, ast.Module | None] = {}
    for m in project.models.values():
        if m.file not in trees:
            trees[m.file] = _parse(project.bound.loaded.project_dir, m.file)
        # the same order as the runtime: the call's dialect=, the model's, the engine's
        queries += [
            (m, sql, set(m.inputs) | extra, loc, dialect or m.config.sql_dialect or engine_dialect)
            for sql, extra, loc, dialect in _literal_queries(trees[m.file], m)
        ]
    for m, sql, aliases, loc, dialect in queries:
        ctx = {"model": m.name}
        try:
            statements = parse(sql, read=dialect)
        except (SqlglotError, ValueError):  # ValueError: an unknown dialect name
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


def sql_change(old: str, new: str, dialect: str | None) -> tuple[str, str]:
    """How a query changed, for `plan --diff` (ADR-0052): `("same", "")`, `("non-breaking",
    why)` when the new query only adds projected columns, or `("breaking", why)`. Anything this
    cannot tell is breaking: sqlglot missing, a query it cannot parse, more than one statement,
    or any change it does not recognize as a pure addition."""
    if importlib.util.find_spec("sqlglot") is None:
        return "breaking", "sqlglot is not installed, so the SQL change is unknown"
    from sqlglot import exp, parse
    from sqlglot.errors import SqlglotError

    try:
        a, b = parse(old, read=dialect), parse(new, read=dialect)
    except (SqlglotError, ValueError):  # ValueError: an unknown dialect name
        return "breaking", "the SQL cannot be parsed"
    if len(a) != 1 or len(b) != 1 or a[0] is None or b[0] is None:
        return "breaking", "the SQL is not one statement"
    before, after = a[0], b[0]

    def text(e: Any) -> str:
        return str(e.sql(dialect=dialect, comments=False))

    if text(before) == text(after):
        return "same", ""
    if not isinstance(before, exp.Select) or not isinstance(after, exp.Select):
        return "breaking", "the query changed"
    olds = [text(e) for e in before.expressions]
    kept: list[exp.Expression] = []
    added: list[exp.Expression] = []
    for e in after.expressions:
        (kept if len(kept) < len(olds) and text(e) == olds[len(kept)] else added).append(e)
    if len(kept) < len(olds):
        return "breaking", "an existing column changed or was removed"
    names = {e.alias_or_name for e in before.expressions}
    aggregate = before.args.get("group") or any(
        agg.find_ancestor(exp.Window) is None
        for e in before.expressions
        for agg in e.find_all(exp.AggFunc)
    )
    if before.args.get("distinct") or aggregate:
        return "breaking", "a column was added to a DISTINCT or aggregate query"
    if any(e.is_star for e in before.expressions):
        return "breaking", "a column was added next to *, so its name may clash"
    # set-returning or row-changing shapes; generate_series returns rows in some dialects
    rows = (exp.Select, exp.Subquery, exp.UDTF, exp.Unnest, exp.Anonymous, exp.GenerateSeries)
    for e in added:
        if not e.alias_or_name or e.alias_or_name in names:
            return "breaking", "an added column has no name or reuses one"
        names.add(e.alias_or_name)
        # ORDER BY, WHERE, windows and the like could now read the new alias
        mine = {id(n) for n in e.walk()}
        used = {c.name for c in after.find_all(exp.Column) if id(c) not in mine}
        if e.alias_or_name in used:
            return "breaking", f"added column '{e.alias_or_name}' shadows a name the query uses"
        if any(
            isinstance(n, rows)
            or (isinstance(n, exp.AggFunc) and n.find_ancestor(exp.Window) is None)
            for n in e.walk()
        ):
            return "breaking", f"added column '{e.alias_or_name}' may change the row count"
    rest = after.copy()
    rest.set("expressions", [e.copy() for e in kept])
    if text(rest) != text(before):
        return "breaking", "the query changed beyond the added columns"
    return "non-breaking", "added " + ", ".join(e.alias_or_name for e in added)


def check_static(pg: ProjectGraph, report: Report) -> None:
    """Report PLK602 (found while binding) and PLK606 for Python models."""
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
    if no_sql is None:
        return
    for m in project.models.values():
        ref = m.entry.python
        name = ref.partition(":")[2]
        file = m.site.file
        if file not in trees:
            try:
                trees[file] = ast.parse((loaded.project_dir / file).read_bytes(), filename=file)
            except (SyntaxError, ValueError) as exc:
                trees[file] = f"{type(exc).__name__}: {exc}"
        tree = trees[file]
        if isinstance(tree, str):
            continue
        last: ast.stmt | None = None
        for stmt in tree.body:
            if (isinstance(stmt, _Def) and stmt.name == name) or name in _bound_names(stmt):
                last = stmt
        # Not a top-level def: discovery imported it (loops and factories, ADR-0050 point 2).
        if isinstance(last, _Def) and _calls_ctx_sql(last):
            report.add(
                "PLK606",
                f"'{ref}' calls ctx.sql, but engine '{no_sql}' has no SQL",
                location=where(project, m, "python"),
                hint="write the model with frames, or choose an engine with SQL",
                context={"model": m.name, "callable": ref, "engine": no_sql},
            )
