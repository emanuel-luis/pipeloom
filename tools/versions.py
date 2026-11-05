"""Keep the versions of the distributions in the workspace in lockstep.

    python tools/versions.py check [--tag vX.Y.Z]
    python tools/versions.py bump X.Y.Z [--no-lock]

`check` verifies that every plugin under `plugins/` has the root version, depends on
`pipeloom~=X.Y.0`, and is pinned `==X.Y.Z` by a root extra (an extra on the name, as in
`pipeloom-spark[local]==X.Y.Z`, is accepted). A member that pins another member
(`pipeloom-spark~=X.Y.0`) must use the same `X.Y.0`. `bump` rewrites those lines, checks,
then runs `uv lock`.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
VERSION = re.compile(r"\d+\.\d+\.\d+")
_VERSION_LINE = re.compile(r'^version = "[^"]*"$', re.MULTILINE)
_CORE_PIN = re.compile(r'"(pipeloom(?:-[a-z0-9-]+)?)~=[^"]*"')
_MEMBER_PIN = re.compile(r"(pipeloom-[a-z0-9-]+)~=(.+)")
_EXTRA_PIN = re.compile(r'"(pipeloom-[a-z0-9-]+(?:\[[a-z0-9,-]+\])?)==[^"]*"')
_PINNED = re.compile(r"(pipeloom-[a-z0-9-]+)(?:\[[a-z0-9,-]+\])?==(.+)")


def _load(path: Path) -> dict[str, Any]:
    with path.open("rb") as fh:
        return tomllib.load(fh)


def members(root: Path) -> list[Path]:
    """The pyproject.toml of every workspace member, sorted by name."""
    return sorted(root.glob("plugins/*/pyproject.toml"), key=lambda p: p.parent.name)


def check(root: Path, tag: str | None = None) -> list[str]:
    """Every inconsistency found, empty when the workspace is in lockstep."""
    errors: list[str] = []
    meta = _load(root / "pyproject.toml")["project"]
    version = meta["version"]
    if not VERSION.fullmatch(version):
        return [f"root version '{version}' is not X.Y.Z"]
    major, minor, _ = version.split(".")
    if tag is not None and tag != f"v{version}":
        errors.append(f"tag {tag} does not match the root version {version}")
    extras = [dep for deps in meta.get("optional-dependencies", {}).values() for dep in deps]
    pinned = {m[1]: m[2] for d in extras if (m := _PINNED.fullmatch(d.replace(" ", "")))}
    for path in members(root):
        project = _load(path)["project"]
        name = project["name"]
        if project["version"] != version:
            errors.append(f"{name} is {project['version']}, the root is {version}")
        core = f"pipeloom~={major}.{minor}.0"
        deps = [d.replace(" ", "") for d in project.get("dependencies", [])]
        if core not in deps:
            errors.append(f"{name} must depend on {core}")
        # A member may pin another member (pipeloom-databricks needs pipeloom-spark).
        for dep in deps:
            if (m := _MEMBER_PIN.fullmatch(dep)) and m[2] != f"{major}.{minor}.0":
                errors.append(f"{name} must depend on {m[1]}~={major}.{minor}.0")
        if name not in pinned:
            errors.append(f"no root extra pins {name}")
        elif pinned[name] != version:
            errors.append(f"the root extra pins {name}=={pinned[name]}, not {version}")
    return errors


def bump(root: Path, version: str, *, lock: bool = True) -> list[str]:
    """Rewrite the version lines, then check; run `uv lock` when the check passes."""
    if not VERSION.fullmatch(version):
        return [f"'{version}' is not X.Y.Z"]
    major, minor, _ = version.split(".")
    files = [root / "pyproject.toml", *members(root)]
    for path in files:
        text = path.read_text(encoding="utf-8")
        text = _VERSION_LINE.sub(f'version = "{version}"', text, count=1)
        text = _CORE_PIN.sub(rf'"\1~={major}.{minor}.0"', text)
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
    p_check.add_argument("--tag", help="also require this git tag, vX.Y.Z")
    p_bump = sub.add_parser("bump", help="set every version to X.Y.Z")
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
