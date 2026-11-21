"""Split checksums (ADR-0052): one digest per part of a model, so `state:modified.<part>` can
tell a code edit from a config, contract, test or upstream change.

The 1.0 `checksum` stays as it is; these digests sit next to it in the manifest and stay out of
the plan hash.
"""

from __future__ import annotations

import ast
import hashlib
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from pipeloom.config.models import ColumnConfig, DataTestConfig
from pipeloom.config.resolve import ResolvedModel, SettingsLayer
from pipeloom.config.yaml import LoadedDocument
from pipeloom.plan.manifest import canonical_json
from pipeloom.plan.model import PlanSql, SqlQuery
from pipeloom.secrets.interpolation import InterpolationRecord

_QUOTED = r"""'(?:[^']|'')*'|"(?:[^"]|"")*"|`[^`]*`"""
# A quoted literal (kept as is), or a run of whitespace and comments (one space).
_SQL = re.compile(rf"({_QUOTED})|(?:\s|--[^\n]*|/\*.*?\*/)+", re.DOTALL)
_VAR = re.compile(r"(?<!\$)\$\{var:([A-Za-z_][A-Za-z0-9_]*)\}")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_sql(text: str) -> str:
    """Comments dropped and whitespace collapsed to one space, outside quotes. Text this cannot
    read safely is returned as is: a backslash (escapes), a `$` (dollar quoting), or a quote
    left over once the quoted literals are taken out (including one inside a comment)."""
    if "\\" in text or "$" in text or re.search("['\"`]", re.sub(_QUOTED, "", text)):
        return text
    return _SQL.sub(lambda m: m.group(1) or " ", text).strip()


def _strip_docstring(body: list[ast.stmt]) -> None:
    first = body[0] if body else None
    if (
        isinstance(first, ast.Expr)
        and isinstance(first.value, ast.Constant)
        and isinstance(first.value.value, str)
    ):
        body[:1] = [] if len(body) > 1 else [ast.Pass()]


def _dump(node: Any) -> Any:
    """The AST as plain data, without positions. None and empty lists are left out, so a field a
    later Python adds with an empty default does not change the digest."""
    if isinstance(node, ast.AST):
        fields = ((k, _dump(v)) for k, v in ast.iter_fields(node))
        return [type(node).__name__, [[k, v] for k, v in fields if v is not None and v != []]]
    if isinstance(node, list):
        return [_dump(v) for v in node]
    if isinstance(node, (bytes, complex)) or node is ...:
        return repr(node)
    if isinstance(node, float) and not math.isfinite(node):  # canonical JSON has no inf or NaN
        return repr(node)
    return node


def _is_model(decorator: ast.expr) -> bool:
    """`@model`, `@model(...)`, `@pipeloom.model` or `@pipeloom.model(...)`."""
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", None)
    return name == "model"


def code_digest(source: bytes, callable_ref: str) -> str:
    """The digest of `module_tree(source)` with the callable."""
    return _sha(canonical(callable_ref, module_tree(source)))


def module_tree(source: bytes) -> Any:
    """The module's syntax tree as data, without docstrings, comments or layout, with a plain
    string passed to any `.sql(...)` call normalized by `normalize_sql`. An f-string is left as
    the tree has it, since its pieces cannot be read as SQL one at a time. `@model` decorators
    are left out: what they set is config, contract, tests or inputs, which other parts cover."""
    return _normalized(ast.parse(source))


def _normalized(tree: ast.Module) -> Any:
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            _strip_docstring(node.body)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            node.decorator_list = [d for d in node.decorator_list if not _is_model(d)]
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
            and node.args
        ):
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                arg.value = normalize_sql(arg.value)
    return _dump(tree)


def _ctx_sql_literal(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "sql"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "ctx"
        and bool(node.args)
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    )


def sql_record(source: bytes, callable_ref: str) -> PlanSql | None:
    """What `plan --diff` needs to compare a SQL model's code (ADR-0052): the literal
    `ctx.sql("...")` queries of the model's function, each with its literal `dialect=`, and
    `shell`, the code digest with those query strings blanked. None when the function has no
    such query, or passes `dialect=` as anything but a string literal."""
    tree = ast.parse(source)
    name = callable_ref.partition(":")[2]
    defs = (ast.FunctionDef, ast.AsyncFunctionDef)
    fns = [s for s in tree.body if isinstance(s, defs) and s.name == name]
    calls = [n for n in ast.walk(fns[-1]) if _ctx_sql_literal(n)] if fns else []
    queries: list[SqlQuery] = []
    for call in calls:
        assert isinstance(call, ast.Call)
        arg = call.args[0]
        assert isinstance(arg, ast.Constant)
        dialect = next((k.value for k in call.keywords if k.arg == "dialect"), None)
        if dialect is not None and not (
            isinstance(dialect, ast.Constant) and isinstance(dialect.value, str)
        ):
            return None
        literal = None if dialect is None else str(dialect.value)
        queries.append(SqlQuery(sql=str(arg.value), dialect=literal))
        arg.value = ""
    if not queries:
        return None
    return PlanSql(shell=_sha(canonical(callable_ref, _normalized(tree))), queries=tuple(queries))


def canonical(*values: Any) -> bytes:
    return canonical_json(list(values))


def _at(data: Any, pointer: str) -> Any:
    for seg in pointer.split("/")[1:]:
        seg = seg.replace("~1", "/").replace("~0", "~")
        data = data[int(seg)] if isinstance(data, list) else data[seg]
    return data


def _written(
    layers: Sequence[SettingsLayer],
    docs: Mapping[str, LoadedDocument],
    envs: Iterable[InterpolationRecord],
    vars_: Mapping[str, Any],
) -> dict[tuple[str, ...], Any]:
    """Settings paths whose winning value came through `${env:...}`, mapped to the value as
    written in the YAML with only `${var:...}` resolved."""
    tainted = [(r.file, r.pointer) for r in envs]
    out: dict[tuple[str, ...], Any] = {}

    def as_written(file: str, pointer: str) -> Any:
        raw = _at(docs[file].data, pointer)
        if isinstance(raw, list):
            return [
                _VAR.sub(lambda m: str(vars_.get(m.group(1), m.group(0))), v)
                if isinstance(v, str)
                else v
                for v in raw
            ]
        if isinstance(raw, str):
            return _VAR.sub(lambda m: str(vars_.get(m.group(1), m.group(0))), raw)
        return raw

    def is_tainted(file: str, pointer: str | None) -> bool:
        return (
            pointer is not None
            and file in docs
            and any(f == file and (p == pointer or p.startswith(pointer + "/")) for f, p in tainted)
        )

    # `tags` is a union of every layer, so when `${env:...}` fills any of them, every layer's
    # list counts as written, in layer order.
    tag_layers = [la for la in layers if "tags" in la.settings]
    if any(is_tainted(la.file, la.pointers.get("tags")) for la in tag_layers):
        out[("tags",)] = [
            as_written(la.file, ptr)
            if (ptr := la.pointers.get("tags")) is not None and la.file in docs
            else la.settings["tags"]
            for la in tag_layers
        ]

    def walk(file: str, path: tuple[str, ...], value: Any, pointer: str | None) -> None:
        if isinstance(value, dict):  # mappings merge, so a lower layer's leaves may survive
            for k, v in value.items():
                sub = (
                    None
                    if pointer is None
                    else f"{pointer}/{k.replace('~', '~0').replace('/', '~1')}"
                )
                walk(file, (*path, k), v, sub)
            return
        for key in [p for p in out if p[: len(path)] == path]:
            del out[key]
        if pointer is not None and is_tainted(file, pointer):
            out[path] = as_written(file, pointer)

    for layer in layers:
        for key, value in layer.settings.items():
            if key != "tags":
                walk(layer.file, (key,), value, layer.pointers.get(key))
    return out


def config_view(
    dumped: dict[str, Any],
    model: ResolvedModel,
    docs: Mapping[str, LoadedDocument],
    records: Sequence[InterpolationRecord],
    vars_: Mapping[str, Any],
) -> dict[str, Any]:
    """The dumped config with every value that `${env:...}` filled put back as written, so
    environment values (and secrets passed through them) are never hashed."""
    envs = [r for r in records if r.kind == "env"]
    if not envs:
        return dumped
    for path, raw in _written(model.layers, docs, envs, vars_).items():
        node: Any = dumped
        for key in path[:-1]:
            node = node.get(key) if isinstance(node, dict) else None
        if isinstance(node, dict) and path[-1] in node:
            node[path[-1]] = raw
    return dumped


def part_digests(
    code: str,
    config: Mapping[str, Any],
    columns: Sequence[ColumnConfig] | None,
    data_tests: Sequence[DataTestConfig] | None,
) -> dict[str, str]:
    """Every part but `upstream`, which needs the parents' digests."""
    return {
        "code": code,
        "config": _sha(canonical(config)),
        "contract": _sha(
            canonical(None if columns is None else [c.model_dump(mode="json") for c in columns])
        ),
        "tests": _sha(
            canonical(
                None if data_tests is None else [t.model_dump(mode="json") for t in data_tests]
            )
        ),
    }


def upstream_digest(inputs: Mapping[str, str], parts: Mapping[str, Mapping[str, str]]) -> str:
    """The direct inputs with each model parent's code, config, contract and upstream digests,
    so a change anywhere above reaches every descendant. A source counts by its id only."""
    rows = []
    for alias, uid in sorted(inputs.items()):
        p = parts.get(uid)
        rows.append(
            [
                alias,
                uid,
                None if p is None else [p[k] for k in ("code", "config", "contract", "upstream")],
            ]
        )
    return _sha(canonical(rows))
