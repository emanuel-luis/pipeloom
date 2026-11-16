"""Convert legacy YAML model declarations to Python-first models (ADR-0050).

    python -m tests.legacy_models <project-dir>...
    python -m tests.legacy_models <case.yml>... --case

A test support utility: `migrate(files)` lets tests keep writing compact YAML-style models,
which `tests.conftest.make_project` converts on write, and the command line migrated the
fixtures and case files in place. It is not a user-facing command. It converts:

- each `sql:` model into a decorated function in a `.py` file next to its YAML file, whose body
  is `return ctx.sql(...)` and whose parameters are the input aliases;
- each `python: module:function` model by decorating that function where it is defined;
- flat `sources:` lists into one source group, `raw`, in `pipeloom.yml`;
- `_defaults.yml` files and `defaults.models` into the `models:` tree with `+` keys;
- `unit_tests` of a model entry, and a file's top-level `unit_tests` list (cases that name
  their `model`), into the model's properties entry in the same file.

Other model settings become decorator keywords. Properties entries stay in their YAML file.
Anything it cannot convert raises `MigrationError` and nothing is written.
"""

from __future__ import annotations

import argparse
import ast
import json
import keyword
import re
import sys
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

GROUP = "raw"
RESERVED = {"model", "ref", "source", "ctx"}
SKIP_DIRS = {".pipeloom", "target", "__pycache__", ".venv"}
_TOP_KEY = re.compile(r"^([A-Za-z_][\w-]*):")


class MigrationError(Exception):
    """A declaration the tool cannot convert."""


def _load(text: str) -> Any:
    """The YAML document, or None for a file that is not valid YAML (or raw bytes in a case)."""
    if not isinstance(text, str):
        return None
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        return None


def _py(value: Any) -> str:
    """A Python literal for a YAML value."""
    if isinstance(value, bool) or value is None:
        return repr(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, int | float):
        return repr(value)
    if isinstance(value, list):
        return "[" + ", ".join(_py(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join(f"{_py(k)}: {_py(v)}" for k, v in value.items()) + "}"
    raise MigrationError(f"cannot write {value!r} as a Python literal")


def _sql_literal(sql: str) -> str:
    if "\n" in sql.strip() and '"""' not in sql and "\\" not in sql and not sql.endswith('"'):
        return '"""' + sql + '"""'
    return json.dumps(sql)


def _merge(base: dict[str, Any], over: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in over.items():
        out[k] = _merge(out[k], v) if isinstance(v, dict) and isinstance(out.get(k), dict) else v
    return out


def _under(path: str, root: str) -> str | None:
    """The path relative to root, or None."""
    p, r = PurePosixPath(path), PurePosixPath(root)
    if str(r) == ".":
        return str(p)
    return str(p.relative_to(r)) if p.is_relative_to(r) else None


def _input_args(
    name: str, inputs: dict[str, Any], params: list[str]
) -> tuple[dict[str, str], set[str]]:
    """The `inputs=` entries (as source text) and the helpers they use. A ref whose alias is a
    parameter named like its target binds by name and needs no entry (never the model itself);
    an alias that is no parameter (a `**kwargs` function) is always listed."""
    out: dict[str, str] = {}
    helpers: set[str] = set()
    for alias, spec in inputs.items():
        if "ref" in spec:
            target = spec["ref"]
            if alias == target and target != name and alias in params:
                continue
            out[alias] = f"ref({_py(target)})"
            helpers.add("ref")
        else:
            group, _, table = spec["source"].rpartition(".")
            out[alias] = f"source({_py(group or GROUP)}, {_py(table)})"
            helpers.add("source")
    return out, helpers


def _decorator(kwargs: dict[str, str]) -> str:
    if not kwargs:
        return "@model\n"
    body = "".join(f"    {k}={v},\n" for k, v in kwargs.items())
    return f"@model(\n{body})\n"


def _settings(entry: dict[str, Any]) -> dict[str, str]:
    skip = {"name", "sql", "python", "inputs"}
    return {k: _py(v) for k, v in entry.items() if k not in skip}


def _sql_model(entry: dict[str, Any]) -> tuple[str, set[str]]:
    name = entry["name"]
    inputs = entry.get("inputs") or {}
    params = list(inputs)
    fn = name
    kwargs: dict[str, str] = {}
    if keyword.iskeyword(name) or name in RESERVED:
        fn = name + "_"
        kwargs["name"] = _py(name)
    entries, helpers = _input_args(name, inputs, params)
    if entries:
        kwargs["inputs"] = "{" + ", ".join(f"{_py(a)}: {v}" for a, v in entries.items()) + "}"
    kwargs.update(_settings(entry))
    sig = ", ".join(["ctx", *params])
    code = (
        f"{_decorator(kwargs)}def {fn}({sig}):\n    return ctx.sql({_sql_literal(entry['sql'])})\n"
    )
    return code, helpers


def _module_name(path: str, roots: list[str]) -> str | None:
    """The module name of a file: its path relative to the longest python_paths root."""
    rels = [r for r in (_under(path, root) for root in roots) if r is not None]
    if not rels:
        return None
    parts = PurePosixPath(min(rels, key=len)).with_suffix("").parts
    return ".".join(parts[:-1] if parts[-1] == "__init__" else parts)


def _module_file(module: str, files: dict[str, str | None], roots: list[str]) -> tuple[str, str]:
    rel = module.replace(".", "/")
    for root in roots:
        base = "" if root == "." else root.rstrip("/") + "/"
        for cand in (f"{base}{rel}.py", f"{base}{rel}/__init__.py"):
            if files.get(cand) is not None:
                return cand, root
    raise MigrationError(f"module '{module}' is not a file of the project")


def _import_line(helpers: set[str]) -> str:
    return "from pipeloom import " + ", ".join(sorted({"model", *helpers})) + "\n"


def _decorate(text: str, path: str, fns: list[tuple[str, dict[str, Any]]]) -> str:
    """Decorate the named top-level functions of one module in place."""
    tree = ast.parse(text, path)
    defs = {n.name: n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    lines = text.splitlines(keepends=True)
    helpers: set[str] = set()
    inserts: list[tuple[int, str]] = []
    for fn_name, entry in fns:
        node = defs.get(fn_name)
        if node is None or node.decorator_list:
            raise MigrationError(f"{path}: no undecorated top-level function '{fn_name}'")
        args = node.args
        names = [a.arg for a in [*args.posonlyargs, *args.args, *args.kwonlyargs]]
        if (names and names[0] != "ctx") or args.vararg is not None or args.posonlyargs:
            raise MigrationError(f"{path}:{fn_name}: the first parameter must be ctx")
        name = entry["name"]
        kwargs: dict[str, str] = {}
        if fn_name != name:
            kwargs["name"] = _py(name)
        inputs = entry.get("inputs") or {}
        params = [n for n in names if n != "ctx"]
        entries, used = _input_args(name, inputs, params)
        helpers |= used
        if entries:
            kwargs["inputs"] = "{" + ", ".join(f"{_py(a)}: {v}" for a, v in entries.items()) + "}"
        kwargs.update(_settings(entry))
        inserts.append((node.lineno - 1, _decorator(kwargs)))
    head = 0
    for stmt in tree.body:
        is_doc = isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant)
        future = isinstance(stmt, ast.ImportFrom) and stmt.module == "__future__"
        if (is_doc and stmt is tree.body[0]) or future:
            head = stmt.end_lineno or head
        else:
            break
    for at, block in sorted(inserts, reverse=True):
        lines.insert(at, block)
    prefix = "\n" if head else ""
    lines.insert(head, prefix + _import_line(helpers) + ("\n" if head < len(lines) else ""))
    return "".join(lines)


def _replace_top_keys(text: str, changes: dict[str, Any]) -> str:
    """Replace, remove (value None) or add top-level keys of a YAML text, keeping other lines."""
    lines = text.splitlines(keepends=True)
    blocks: list[tuple[str | None, list[str]]] = [(None, [])]
    for line in lines:
        m = _TOP_KEY.match(line)
        if m:
            blocks.append((m.group(1), [line]))
        else:
            blocks[-1][1].append(line)
    out: list[str] = []
    done: set[str] = set()

    def dump(key: str) -> list[str]:
        done.add(key)
        if changes[key] is None:
            return []
        return [yaml.safe_dump({key: changes[key]}, sort_keys=False, default_flow_style=False)]

    for key, block in blocks:
        if key == "targets":
            for k in changes:
                if k not in done and k not in {b[0] for b in blocks}:
                    out += dump(k)
        if key in changes:
            out += dump(key)
        else:
            out += block
    for k in changes:
        if k not in done:
            out += dump(k)
    result = "".join(out)
    return result if result.endswith("\n") else result + "\n"


def migrate(files: dict[str, str]) -> dict[str, str]:
    """The project files after migration, in order: changed files in place, a generated module
    right after the YAML file it came from, removed files left out."""
    project = _load(files.get("pipeloom.yml", ""))
    if not isinstance(project, dict):  # no project file, or one a case breaks on purpose
        return dict(files)
    model_paths: list[str] = list(project.get("model_paths", ["models"]))
    python_paths: list[str] = list(project.get("python_paths", ["."]))
    new: dict[str, str | None] = dict(files)
    after: dict[str, list[str]] = {}
    tree: dict[str, Any] = {}
    tables: dict[str, Any] = {}
    py_models: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    changed = False

    def roots_of(path: str) -> str | None:
        for root in model_paths:
            rel = _under(path, root)
            if rel is not None:
                return rel
        return None

    for path, text in files.items():
        if not path.endswith((".yml", ".yaml")) or path == "pipeloom.yml":
            continue
        rel = roots_of(path)
        if rel is None:
            continue
        data = _load(text)
        if not isinstance(data, dict):
            continue
        if PurePosixPath(path).stem == "_defaults":
            node = tree
            for part in PurePosixPath(rel).parent.parts:
                node = node.setdefault(part, {})
            for k, v in (data.get("models") or {}).items():
                node["+" + k] = _merge(node.get("+" + k, {}), v) if isinstance(v, dict) else v
            new[path] = None
            changed = True
            continue
        sources = data.pop("sources", None) or []
        for s in sources:
            s = dict(s)
            table = s.pop("name")
            if table in tables:
                raise MigrationError(f"source '{table}' is declared twice")
            tables[table] = s
        # Unit tests live only in properties entries: a model entry's own list and the file's
        # top-level list (whose cases name their `model`) move there.
        cases: dict[str, list[Any]] = {}
        for entry in data.get("models") or []:
            if (
                isinstance(entry, dict)
                and {"sql", "python"} & entry.keys()
                and "unit_tests" in entry
            ):
                cases.setdefault(entry["name"], []).extend(entry.pop("unit_tests") or [])
        for case in data.pop("unit_tests", None) or []:
            case = dict(case)
            cases.setdefault(case.pop("model"), []).append(case)
        kept, code, helpers = [], [], set()
        for entry in data.get("models") or []:
            if isinstance(entry, dict) and "sql" in entry and "python" not in entry:
                body, used = _sql_model(entry)
                code.append(body)
                helpers |= used
            elif isinstance(entry, dict) and "python" in entry and "sql" not in entry:
                module, _, fn = entry["python"].partition(":")
                mod_path, root = _module_file(module, new, python_paths)
                py_models.setdefault(mod_path, []).append((fn, entry))
                if all(_under(mod_path, r) is None for r in model_paths):
                    if root == ".":
                        raise MigrationError(f"{mod_path} is outside model_paths")
                    model_paths.append(root)
            else:
                kept.append(entry)
        for name, tests in cases.items():
            prop = next((e for e in kept if isinstance(e, dict) and e.get("name") == name), None)
            if prop is None:
                kept.append({"name": name, "unit_tests": tests})
            else:
                prop["unit_tests"] = [*(prop.get("unit_tests") or []), *tests]
        if not sources and not code and not cases and len(kept) == len(data.get("models") or []):
            continue
        changed = True
        if code:
            target = str(PurePosixPath(path).with_suffix(".py"))
            taken = {_module_name(p, python_paths) for p in new if p.endswith(".py")}
            if target in files or _module_name(target, python_paths) in taken:
                target = str(PurePosixPath(path).with_name(PurePosixPath(path).stem + "_sql.py"))
            if target in files:
                raise MigrationError(f"{target} already exists")
            new[target] = _import_line(helpers) + "\n\n" + "\n\n".join(code)
            after.setdefault(path, []).append(target)
            if all(_under(target, r) is None for r in python_paths):
                python_paths.append(".")
        if kept:
            data["models"] = kept
        else:
            data.pop("models", None)
        new[path] = (
            yaml.safe_dump(data, sort_keys=False, default_flow_style=False) if data else None
        )
    for mod_path, fns in py_models.items():
        if len({fn for fn, _ in fns}) != len(fns):
            raise MigrationError(f"{mod_path}: one function backs several models")
        text = new[mod_path]
        assert text is not None
        new[mod_path] = _decorate(text, mod_path, fns)
    defaults = (project.get("defaults") or {}).get("models") or {}
    if not changed and not defaults and not py_models:
        return dict(files)
    changes: dict[str, Any] = {}
    if defaults:
        root_keys = {"+" + k: v for k, v in defaults.items()}
        tree = _merge(root_keys, tree)
    if defaults or "defaults" in project:
        changes["defaults"] = None
    if tree:
        changes["models"] = _merge(tree, project.get("models") or {})
    if tables:
        groups = dict(project.get("sources") or {})
        if GROUP in groups:
            raise MigrationError(f"pipeloom.yml already has a source group '{GROUP}'")
        groups[GROUP] = {"tables": tables}
        changes["sources"] = groups
    left = [p for p, v in new.items() if v is not None and p != "pipeloom.yml"]
    # Drop a root that held files before and holds none now; a root that was empty stays.
    model_paths = [
        r
        for r in model_paths
        if any(_under(p, r) is not None for p in left)
        or not any(_under(p, r) is not None for p in files if p != "pipeloom.yml")
    ]
    if model_paths != list(project.get("model_paths", ["models"])):
        changes["model_paths"] = model_paths
    if python_paths != list(project.get("python_paths", ["."])):
        changes["python_paths"] = python_paths
    if changes:
        new["pipeloom.yml"] = _replace_top_keys(files["pipeloom.yml"], changes)
    out: dict[str, str] = {}
    for path, value in new.items():
        if path in files and value is not None:
            out[path] = value
        for extra in after.get(path, []):
            out[extra] = new[extra]  # type: ignore[assignment]
    return out


def _literal(key: str, content: Any) -> str:
    if not isinstance(content, str):  # {"base64": ...}
        dumped = yaml.safe_dump({key: content}, sort_keys=False, default_flow_style=False)
        return "".join("  " + ln for ln in dumped.splitlines(keepends=True))
    plain = re.fullmatch(r"[\w./-]+", key) is not None
    head = f"  {key if plain else json.dumps(key)}:"
    if content == "":
        return f'{head} ""\n'
    chomp = "+" if content.endswith("\n\n") else "" if content.endswith("\n") else "-"
    indent = "4" if content[:1] in (" ", "\n") else ""
    body = "".join(
        ("    " + ln if ln.strip() else "\n") if ln.endswith("\n") else "    " + ln
        for ln in content.splitlines(keepends=True)
    )
    return f"{head} |{indent}{chomp}\n{body}" + ("" if body.endswith("\n") else "\n")


def migrate_case(text: str) -> str:
    """Migrate the `files:` mapping of a case file, keeping its other lines."""
    spec = yaml.safe_load(text)
    files = spec.get("files") or {}
    out = migrate(files)
    if out == files:
        return text
    lines = text.splitlines(keepends=True)
    start = next(i for i, ln in enumerate(lines) if ln.startswith("files:"))
    end = next((i for i in range(start + 1, len(lines)) if _TOP_KEY.match(lines[i])), len(lines))
    block = "files:\n" + "".join(_literal(k, v) for k, v in out.items())
    result = "".join(lines[:start]) + block + "".join(lines[end:])
    if (yaml.safe_load(result) or {}).get("files") != out:
        raise MigrationError("the rewritten files: block does not read back the same")
    return result


def _read_project(root: Path) -> dict[str, str]:
    files: dict[str, str] = {}
    for p in sorted(root.rglob("*")):
        rel = p.relative_to(root)
        if p.is_file() and p.suffix in {".yml", ".yaml", ".py"} and not SKIP_DIRS & set(rel.parts):
            files[rel.as_posix()] = p.read_text(encoding="utf-8")
    return files


def migrate_dir(root: Path) -> None:
    files = _read_project(root)
    out = migrate(files)
    for rel in files.keys() - out.keys():
        (root / rel).unlink()
    for rel, text in out.items():
        if files.get(rel) != text:
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(text, encoding="utf-8", newline="\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("paths", type=Path, nargs="+")
    parser.add_argument("--case", action="store_true", help="the paths are case files")
    args = parser.parse_args(argv)
    status = 0
    for path in args.paths:
        try:
            if args.case:
                text = path.read_text(encoding="utf-8")
                new = migrate_case(text)
                if new != text:
                    path.write_text(new, encoding="utf-8", newline="\n")
            else:
                migrate_dir(path)
        except MigrationError as err:
            print(f"{path}: {err}", file=sys.stderr)
            status = 1
    return status


if __name__ == "__main__":
    raise SystemExit(main())
