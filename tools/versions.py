"""Keep the versions of the distributions in the workspace in lockstep.

    python tools/versions.py check [--tag vX.Y.Z]
    python tools/versions.py bump X.Y.Z [--no-lock]

A version is `X.Y.Z` or a PEP 440 prerelease `X.Y.Z(a|b|rc)N` such as `1.0.0rc1`.

`check` verifies that every plugin under `plugins/` has the root version, depends on the
core pin, and is pinned `==<version>` by a root extra (an extra on the name, as in
`pipeloom-spark[local]==X.Y.Z`, is accepted). The core pin is `pipeloom~=X.Y.0` for a final
version and `pipeloom~=X.Y.ZrcN` for a prerelease (`~=1.0.0rc1` means `>=1.0.0rc1, ==1.0.*`,
so it accepts 1.0.0rc1 and 1.0.0, while `~=1.0.0` rejects the rc). A member that pins another
member (`pipeloom-spark~=X.Y.0`) must use the same pin.

`check` also reads the source with `ast` (pipeloom is not imported) and requires that:

- the root `[tool.pipeloom]` keys `config-version` and `plugin-api` equal `CONFIG_VERSION`
  and `API_VERSION` in the source;
- every member declares the same two keys as the root;
- every `PluginSpec(...)` under `src/pipeloom` and `plugins/*/src` passes `API_VERSION`
  (as `api_version=API_VERSION` or as the fourth positional argument).

`bump` rewrites the version lines and pins, checks, then runs `uv lock`.
"""

from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
VERSION = re.compile(r"\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?")
_FORMAT = "X.Y.Z or X.Y.Z(a|b|rc)N"
_VERSION_LINE = re.compile(r'^version = "[^"]*"$', re.MULTILINE)
_CORE_PIN = re.compile(r'"(pipeloom(?:-[a-z0-9-]+)?)~=[^"]*"')
_MEMBER_PIN = re.compile(r"(pipeloom-[a-z0-9-]+)~=(.+)")
_EXTRA_PIN = re.compile(r'"(pipeloom-[a-z0-9-]+(?:\[[a-z0-9,-]+\])?)==[^"]*"')
_PINNED = re.compile(r"(pipeloom-[a-z0-9-]+)(?:\[[a-z0-9,-]+\])?==(.+)")
_API_SOURCE = Path("src/pipeloom/plugins/__init__.py")
_CONFIG_SOURCE = Path("src/pipeloom/config/types.py")


def _load(path: Path) -> dict[str, Any]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def members(root: Path) -> list[Path]:
    """The pyproject.toml of every workspace member, sorted by name."""
    return sorted(root.glob("plugins/*/pyproject.toml"), key=lambda p: p.parent.name)


def _pin(version: str) -> str:
    """What `pipeloom~=` takes: X.Y.0 for a final version, the version itself for a prerelease."""
    major, minor, patch = version.split(".")
    return f"{major}.{minor}.0" if patch.isdigit() else version


def _constant(path: Path, name: str) -> Any:
    """The literal assigned to a module-level `name`, read with ast; None when absent."""
    if not path.is_file():
        return None
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        targets = (
            [node.target]
            if isinstance(node, ast.AnnAssign)
            else node.targets
            if isinstance(node, ast.Assign)
            else []
        )
        if any(getattr(t, "id", None) == name for t in targets):
            value = getattr(node, "value", None)
            return ast.literal_eval(value) if value is not None else None
    return None


def _declared(path: Path) -> dict[str, Any] | None:
    table = _load(path).get("tool", {}).get("pipeloom")
    return table if isinstance(table, dict) else None


def _is_api_version(node: ast.expr) -> bool:
    return isinstance(node, ast.Name) and node.id == "API_VERSION"


def _spec_errors(root: Path) -> list[str]:
    """One line for every PluginSpec(...) call that does not pass API_VERSION."""
    errors: list[str] = []
    files = [*root.glob("src/pipeloom/**/*.py"), *root.glob("plugins/*/src/**/*.py")]
    for path in sorted(files):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            func = node.func.value if isinstance(node.func, ast.Subscript) else node.func
            if (getattr(func, "id", None) or getattr(func, "attr", None)) != "PluginSpec":
                continue
            keyword = next((k.value for k in node.keywords if k.arg == "api_version"), None)
            positional = node.args[3] if len(node.args) > 3 else None
            given = keyword if keyword is not None else positional
            if given is None or not _is_api_version(given):
                where = f"{path.relative_to(root).as_posix()}:{node.lineno}"
                errors.append(f"{where}: PluginSpec must pass api_version=API_VERSION")
    return errors


def _declaration_errors(root: Path) -> list[str]:
    errors: list[str] = []
    config = _constant(root / _CONFIG_SOURCE, "CONFIG_VERSION")
    api = _constant(root / _API_SOURCE, "API_VERSION")
    if config is None:
        errors.append(f"CONFIG_VERSION not found in {_CONFIG_SOURCE.as_posix()}")
    if api is None:
        errors.append(f"API_VERSION not found in {_API_SOURCE.as_posix()}")
    expected = {
        "config-version": config,
        "plugin-api": None if api is None else ".".join(str(n) for n in api),
    }
    root_table = _declared(root / "pyproject.toml")
    if root_table is None:
        errors.append("the root pyproject.toml has no [tool.pipeloom] table")
    else:
        for key, want in expected.items():
            if want is not None and root_table.get(key) != want:
                errors.append(
                    f"the root [tool.pipeloom] {key} is {root_table.get(key)!r}, "
                    f"the source has {want!r}"
                )
    for path in members(root):
        name = _load(path)["project"]["name"]
        table = _declared(path)
        if table is None:
            errors.append(f"{name} has no [tool.pipeloom] table")
            continue
        for key in expected:
            ref = (root_table or {}).get(key)
            if table.get(key) != ref:
                errors.append(
                    f"{name} [tool.pipeloom] {key} is {table.get(key)!r}, the root has {ref!r}"
                )
    return errors + _spec_errors(root)


def check(root: Path, tag: str | None = None) -> list[str]:
    """Every inconsistency found, empty when the workspace is in lockstep."""
    errors: list[str] = []
    meta = _load(root / "pyproject.toml")["project"]
    version = meta["version"]
    if not VERSION.fullmatch(version):
        return [f"root version '{version}' is not {_FORMAT}"]
    pin = _pin(version)
    if tag is not None and tag != f"v{version}":
        errors.append(f"tag {tag} does not match the root version {version}")
    extras = [dep for deps in meta.get("optional-dependencies", {}).values() for dep in deps]
    pinned = {m[1]: m[2] for d in extras if (m := _PINNED.fullmatch(d.replace(" ", "")))}
    for path in members(root):
        project = _load(path)["project"]
        name = project["name"]
        if project["version"] != version:
            errors.append(f"{name} is {project['version']}, the root is {version}")
        core = f"pipeloom~={pin}"
        deps = [d.replace(" ", "") for d in project.get("dependencies", [])]
        if core not in deps:
            errors.append(f"{name} must depend on {core}")
        # A member may pin another member (pipeloom-databricks needs pipeloom-spark).
        for dep in deps:
            if (m := _MEMBER_PIN.fullmatch(dep)) and m[2] != pin:
                errors.append(f"{name} must depend on {m[1]}~={pin}")
        if name not in pinned:
            errors.append(f"no root extra pins {name}")
        elif pinned[name] != version:
            errors.append(f"the root extra pins {name}=={pinned[name]}, not {version}")
    return errors + _declaration_errors(root)


def bump(root: Path, version: str, *, lock: bool = True) -> list[str]:
    """Rewrite the version lines, then check; run `uv lock` when the check passes."""
    if not VERSION.fullmatch(version):
        return [f"'{version}' is not {_FORMAT}"]
    pin = _pin(version)
    files = [root / "pyproject.toml", *members(root)]
    for path in files:
        text = path.read_text(encoding="utf-8")
        text = _VERSION_LINE.sub(f'version = "{version}"', text, count=1)
        text = _CORE_PIN.sub(rf'"\1~={pin}"', text)
        text = _EXTRA_PIN.sub(rf'"\1=={version}"', text)
        path.write_text(text, encoding="utf-8", newline="\n")
    errors = check(root)
    if not errors and lock:
        subprocess.run(["uv", "lock"], cwd=root, check=True)
    return errors


def main(argv: list[str] | None = None, root: Path = ROOT) -> int:
    parser = argparse.ArgumentParser(prog="versions.py", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p_check = sub.add_parser("check", help="verify the versions are in lockstep")
    p_check.add_argument("--tag", help="also require this git tag, vX.Y.Z or vX.Y.ZrcN")
    p_bump = sub.add_parser("bump", help="set every version to X.Y.Z or X.Y.ZrcN")
    p_bump.add_argument("version")
    p_bump.add_argument("--no-lock", action="store_true", help="skip uv lock")
    args = parser.parse_args(argv)
    errors = (
        check(root, args.tag)
        if args.command == "check"
        else bump(root, args.version, lock=not args.no_lock)
    )
    for error in errors:
        print(f"versions: {error}", file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
