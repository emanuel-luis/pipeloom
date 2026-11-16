"""Read `@model` from a file's AST without running it (ADR-0050 point 2).

A file is `none` (no models, or it never imports pipeloom), `static` (every model is a literal
decorator on a top-level `def` whose arguments are static) or `dynamic` (it must be imported).

Static decorator arguments are first encoded as a JSON tree (`encode`) and then turned into values
by calling pipeloom's own helpers and config classes on the literal arguments (`decode`). The
discovery cache stores the tree, so a cached file decodes exactly as a freshly parsed one. Tags:
a JSON list is a list; a JSON object is always tagged: `$tuple`, `$set`, `$dict` (a list of
key, value pairs), `$const` (the repr of a bytes or complex literal) and `$call` (a helper or
config class with its encoded `args`, `kwargs` and `at`, the call's line and column). `var()`
decodes to its `Var` marker, never a value.
"""

from __future__ import annotations

import ast
import inspect
import typing
from collections import Counter
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from pipeloom import authoring
from pipeloom.diagnostics import suggest
from pipeloom.discovery.model import DiscoveredModel, DiscoveryFailure, Param

HELPERS = frozenset(
    {
        "ref",
        "source",
        "var",
        "not_null",
        "unique",
        "accepted_values",
        "between",
        "regex",
        "row_count",
        "freshness",
        "Write",
        "Incremental",
        "Column",
        "Expectation",
        "Quality",
        "Deletes",
    }
)
MODULES = ("pipeloom", "pipeloom.authoring")
MODEL_KEYWORDS = frozenset(
    p.name
    for o in typing.get_overloads(authoring.model)
    for p in inspect.signature(o).parameters.values()
    if p.kind is inspect.Parameter.KEYWORD_ONLY
)

Loc = tuple[int, int]
FileKind = Literal["none", "static", "dynamic"]


@dataclass(frozen=True, slots=True)
class Site:
    """A literal `@model` decorator on a top-level `def`: its location and its arguments'."""

    line: int
    col: int
    kwarg_locations: dict[str, Loc]


@dataclass(frozen=True, slots=True)
class StaticModel:
    """A model read from the AST; `kwargs` holds encoded trees."""

    attr: str
    site: Site
    kwargs: dict[str, Any]
    params: tuple[Param, ...]


@dataclass
class ParsedFile:
    kind: FileKind
    models: list[StaticModel] = field(default_factory=list)
    sites: dict[str, Site] = field(default_factory=dict)
    failures: list[DiscoveryFailure] = field(default_factory=list)


class NotStatic(Exception):
    pass


class InvalidArgument(Exception):
    def __init__(self, at: Loc, message: str) -> None:
        super().__init__(message)
        self.at = at


def _loc(node: ast.expr | ast.keyword) -> Loc:
    return node.lineno, node.col_offset + 1


class _Names:
    """The names a module binds to pipeloom, collected from its module-scope imports."""

    def __init__(self) -> None:
        self.model: set[str] = set()
        self.helpers: dict[str, str] = {}
        self.modules: dict[str, str] = {}

    def qual(self, node: ast.expr) -> str | None:
        """`model` or a helper name when `node` refers to one, else None."""
        if isinstance(node, ast.Name):
            return "model" if node.id in self.model else self.helpers.get(node.id)
        parts: list[str] = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if not parts or not isinstance(node, ast.Name) or node.id not in self.modules:
            return None
        dotted = ".".join([self.modules[node.id], *reversed(parts)])
        mod, _, name = dotted.rpartition(".")
        return name if mod in MODULES and (name == "model" or name in HELPERS) else None

    def encode(self, node: ast.expr) -> Any:
        """The JSON tree of a static expression; raises NotStatic otherwise."""
        if isinstance(node, ast.Call):
            name = self.qual(node.func)
            if name not in HELPERS or any(k.arg is None for k in node.keywords):
                raise NotStatic
            return {
                "$call": name,
                "args": [self.encode(a) for a in node.args],
                "kwargs": {k.arg: self.encode(k.value) for k in node.keywords if k.arg},
                "at": list(_loc(node)),
            }
        if isinstance(node, ast.List):
            return [self.encode(e) for e in node.elts]
        if isinstance(node, ast.Tuple | ast.Set):
            tag = "$tuple" if isinstance(node, ast.Tuple) else "$set"
            return {tag: [self.encode(e) for e in node.elts]}
        if isinstance(node, ast.Dict):
            if any(k is None for k in node.keys):
                raise NotStatic
            pairs = zip(node.keys, node.values, strict=True)
            return {"$dict": [[self.encode(k), self.encode(v)] for k, v in pairs if k is not None]}
        try:
            value = ast.literal_eval(node)
        except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
            raise NotStatic from None
        if value is None or isinstance(value, bool | int | float | str):
            return value
        if isinstance(value, bytes | complex):
            return {"$const": repr(value)}
        raise NotStatic


def decode(tree: Any) -> Any:
    """The value of an encoded tree; helper calls run here. Raises InvalidArgument."""
    if isinstance(tree, list):
        return [decode(e) for e in tree]
    if not isinstance(tree, dict):
        return tree
    if "$tuple" in tree:
        return tuple(decode(e) for e in tree["$tuple"])
    if "$set" in tree:
        return {decode(e) for e in tree["$set"]}
    if "$dict" in tree:
        return {decode(k): decode(v) for k, v in tree["$dict"]}
    if "$const" in tree:
        return ast.literal_eval(tree["$const"])
    name = tree["$call"]
    args = [decode(a) for a in tree["args"]]
    kwargs = {k: decode(v) for k, v in tree["kwargs"].items()}
    try:
        return getattr(authoring, name)(*args, **kwargs)
    except (TypeError, ValueError) as exc:  # pydantic's ValidationError is a ValueError
        line, col = tree["at"]
        raise InvalidArgument((line, col), f"invalid {name}(...): {exc}") from None


def _module_scope(body: Sequence[ast.stmt]) -> Iterator[ast.stmt]:
    """Statements that run at module scope, including those nested in if, try, with and loops."""
    for stmt in body:
        yield stmt
        if not isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            for name in ("body", "orelse", "finalbody"):
                yield from _module_scope(getattr(stmt, name, []))
            for handler in getattr(stmt, "handlers", []):
                yield from _module_scope(handler.body)
            for case in getattr(stmt, "cases", []):
                yield from _module_scope(case.body)


def _bound_names(stmt: ast.stmt) -> Iterator[str]:
    """Names a module-scope statement binds, not looking into nested scopes."""
    if isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
        yield stmt.name
        return
    if isinstance(stmt, ast.Import | ast.ImportFrom):
        for a in stmt.names:
            yield a.asname or a.name.partition(".")[0]
        return
    stack: list[ast.AST] = [stmt]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store | ast.Del):
            yield node.id
        elif isinstance(node, ast.ExceptHandler) and node.name:
            yield node.name
        if node is stmt or not isinstance(node, ast.stmt):
            skip = (ast.Lambda, ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)
            stack.extend(c for c in ast.iter_child_nodes(node) if not isinstance(c, skip))


def _params(args: ast.arguments) -> tuple[Param, ...]:
    positional = [*args.posonlyargs, *args.args]
    first_default = len(positional) - len(args.defaults)
    out = [
        Param(
            a.arg,
            "POSITIONAL_ONLY" if i < len(args.posonlyargs) else "POSITIONAL_OR_KEYWORD",
            i >= first_default,
        )
        for i, a in enumerate(positional)
    ]
    if args.vararg:
        out.append(Param(args.vararg.arg, "VAR_POSITIONAL"))
    out += [
        Param(a.arg, "KEYWORD_ONLY", d is not None)
        for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=True)
    ]
    if args.kwarg:
        out.append(Param(args.kwarg.arg, "VAR_KEYWORD"))
    return tuple(out)


def parse_file(source: bytes, path: str) -> ParsedFile:
    """Classify one file and read its static models. `path` is used in failures only."""
    if b"pipeloom" not in source:
        return ParsedFile("none")
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        return ParsedFile(
            "none", failures=[DiscoveryFailure(path, exc.lineno, exc.offset, "syntax", exc.msg)]
        )
    except ValueError as exc:  # null bytes
        return ParsedFile("none", failures=[DiscoveryFailure(path, None, None, "syntax", str(exc))])

    names = _Names()
    imports_pipeloom = dynamic = False
    scope = list(_module_scope(tree.body))
    in_scope = {id(s) for s in scope}
    pipeloom_imports: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            hits = [a for a in node.names if a.name.partition(".")[0] == "pipeloom"]
        elif isinstance(node, ast.ImportFrom):
            hits = (
                node.names
                if node.level == 0 and (node.module or "").partition(".")[0] == "pipeloom"
                else []
            )
        else:
            continue
        if not hits:
            continue
        imports_pipeloom = True
        pipeloom_imports.add(id(node))
        if id(node) not in in_scope:
            dynamic = True  # pipeloom imported inside a function or class
        for a in hits:
            if isinstance(node, ast.Import):
                names.modules[a.asname or "pipeloom"] = a.name if a.asname else "pipeloom"
            elif a.name == "*":
                dynamic = True
            elif node.module in MODULES and a.name == "model":
                names.model.add(a.asname or a.name)
            elif node.module in MODULES and a.name in HELPERS:
                names.helpers[a.asname or a.name] = a.name
            elif node.module == "pipeloom" and a.name == "authoring":
                names.modules[a.asname or a.name] = "pipeloom.authoring"
    if not imports_pipeloom:
        return ParsedFile("none")

    tracked = names.model | names.helpers.keys() | names.modules.keys()
    rebound = any(
        n in tracked for s in scope if id(s) not in pipeloom_imports for n in _bound_names(s)
    )
    rebound |= any(
        isinstance(n, ast.Global) and tracked.intersection(n.names) for n in ast.walk(tree)
    )
    dynamic |= rebound

    out = ParsedFile("none")
    allowed: set[int] = set()
    defs: list[tuple[ast.FunctionDef | ast.AsyncFunctionDef, ast.expr]] = []
    for stmt in tree.body:
        if not isinstance(stmt, ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        for dec in stmt.decorator_list:
            target = dec.func if isinstance(dec, ast.Call) else dec
            if names.qual(target) == "model":
                allowed.add(id(target))
                defs.append((stmt, dec))
                if len(stmt.decorator_list) > 1:
                    dynamic = True  # another decorator may wrap the marked function

    # A marked name bound again at module scope: the earlier or the wrapped function may be
    # what the module ends up holding, so only an import can tell.
    bindings = Counter(n for s in scope for n in _bound_names(s))
    if any(bindings[fn.name] > 1 for fn, _ in defs):
        dynamic = True

    bad_defs: set[int] = set()
    def_names = {id(dec): fn.name for fn, dec in defs}
    for node in ast.walk(tree):
        if isinstance(node, ast.Name | ast.Attribute) and names.qual(node) == "model":
            if isinstance(node, ast.Name) and not isinstance(node.ctx, ast.Load):
                continue
            if id(node) not in allowed:
                dynamic = True  # factories, loops, model(...)(fn)
        if isinstance(node, ast.Call) and names.qual(node.func) == "model":
            literal = next((k.value for k in node.keywords if k.arg == "name"), None)
            known = (
                literal.value
                if isinstance(literal, ast.Constant) and isinstance(literal.value, str)
                else def_names.get(id(node))
            )
            for kw in node.keywords:
                if kw.arg is not None and kw.arg not in MODEL_KEYWORDS:
                    hint = suggest(kw.arg, sorted(MODEL_KEYWORDS))
                    msg = f"unknown @model argument '{kw.arg}'" + (f"; {hint}" if hint else "")
                    out.failures.append(
                        DiscoveryFailure(path, *_loc(kw), "unknown_keyword", msg, model=known)
                    )
                    bad_defs.add(id(node))

    for fn, dec in defs:
        call = dec if isinstance(dec, ast.Call) else None
        kws = [k for k in call.keywords if k.arg is not None] if call else []
        site = Site(*_loc(dec), {k.arg: _loc(k) for k in kws if k.arg})
        out.sites[fn.name] = site
        if dynamic or id(call) in bad_defs:
            continue
        if call and (call.args or len(kws) != len(call.keywords)):
            dynamic = True
            continue
        try:
            encoded = {k.arg: names.encode(k.value) for k in kws if k.arg}
        except NotStatic:
            dynamic = True
            continue
        kwargs = {k: v for k, v in encoded.items() if v is not None}
        locs = {k: v for k, v in site.kwarg_locations.items() if k in kwargs}
        out.models.append(
            StaticModel(fn.name, Site(site.line, site.col, locs), kwargs, _params(fn.args))
        )

    if dynamic:
        out.kind, out.models = "dynamic", []
    elif out.models or out.failures:
        out.kind = "static"
    return out


def build_models(
    parsed: ParsedFile, path: str, module: str
) -> tuple[list[DiscoveredModel], list[DiscoveryFailure]]:
    """Decode the static models of a parsed file."""
    models: list[DiscoveredModel] = []
    failures: list[DiscoveryFailure] = []
    for m in parsed.models:
        try:
            literal = decode(m.kwargs["name"]) if "name" in m.kwargs else None
        except (InvalidArgument, TypeError):
            literal = None
        known = literal if isinstance(literal, str) else m.attr
        try:
            kwargs = {k: decode(v) for k, v in m.kwargs.items()}
        except InvalidArgument as exc:
            failures.append(
                DiscoveryFailure(path, *exc.at, "invalid_argument", str(exc), model=known)
            )
            continue
        except TypeError as exc:  # an unhashable set member or dict key
            failures.append(
                DiscoveryFailure(
                    path, m.site.line, m.site.col, "invalid_argument", str(exc), model=known
                )
            )
            continue
        name = kwargs.get("name")
        models.append(
            DiscoveredModel(
                name=name if isinstance(name, str) else m.attr,
                module=module,
                attr=m.attr,
                file=path,
                line=m.site.line,
                col=m.site.col,
                kwargs=kwargs,
                kwarg_locations=m.site.kwarg_locations,
                params=m.params,
                dynamic=False,
            )
        )
    return models, failures
