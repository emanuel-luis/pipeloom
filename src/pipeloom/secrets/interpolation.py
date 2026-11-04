"""Compile-time interpolation of `${env:...}` and `${var:...}` in loaded YAML.

Grammar:
- `${env:NAME}` and `${env:NAME:-default}`; the default applies only when NAME is unset.
- `${var:name}`; a value that is exactly one `${var:name}` keeps the variable's type.
- `${secret:provider/path}` is left as is: it resolves at runtime. Only references written
  literally in the YAML count: each is recorded as a `secret` record with its span in the
  result string, so an escaped `$${secret:...}` or text that came from `${env:...}` or
  `${var:...}` never resolves.
- `$${` is an escape for a literal `${`.
- Any other `${...}` is left alone, so Spark-style `${x}` passes through.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from pipeloom.config.yaml import LoadedDocument
from pipeloom.diagnostics import Report, suggest

_TOKEN = re.compile(r"\$\$\{|\$\{([^}]*)\}|\$\{(?:env|var|secret):[^}]*\Z")
_ENV = re.compile(r"^env:([A-Za-z_][A-Za-z0-9_]*)(?::-(.*))?$", re.DOTALL)
_VAR = re.compile(r"^var:([A-Za-z_][A-Za-z0-9_]*)$")
_WHOLE_VAR = re.compile(r"^\$\{var:([A-Za-z_][A-Za-z0-9_]*)\}$")


@dataclass(frozen=True)
class InterpolationRecord:
    pointer: str
    file: str
    kind: Literal["env", "var", "secret"]
    name: str
    default_used: bool
    span: tuple[int, int] | None = None
    """For `secret`: where the reference sits in the interpolated string."""


def _escape(segment: str) -> str:
    return segment.replace("~", "~0").replace("/", "~1")


class _Interpolator:
    def __init__(
        self,
        doc: LoadedDocument,
        env: Mapping[str, str],
        vars_: Mapping[str, Any],
        report: Report,
        skip: Callable[[str], bool],
    ) -> None:
        self.doc, self.env, self.vars, self.report, self.skip = doc, env, vars_, report, skip
        self.records: list[InterpolationRecord] = []

    def walk(self, node: Any, pointer: str) -> Any:
        if isinstance(node, dict):
            return {k: self.walk(v, f"{pointer}/{_escape(k)}") for k, v in node.items()}
        if isinstance(node, list):
            return [self.walk(v, f"{pointer}/{i}") for i, v in enumerate(node)]
        if isinstance(node, str) and "${" in node:
            # A skipped value (a secret-like field) still gets its secret references recorded
            # and its escapes undone; `${env:...}` and `${var:...}` in it stay as written.
            return self.text(node, pointer, only_secret=self.skip(pointer))
        return node

    def text(self, value: str, pointer: str, *, only_secret: bool = False) -> Any:
        whole = _WHOLE_VAR.match(value)
        if not only_secret and whole and whole.group(1) in self.vars:
            self.records.append(
                InterpolationRecord(pointer, self.doc.path, "var", whole.group(1), False)
            )
            return self.vars[whole.group(1)]

        def repl(m: re.Match[str]) -> str:
            if m.group(0) == "$${":
                return "${"
            if only_secret and not m.group(0).startswith("${secret:"):
                return m.group(0)
            if m.group(1) is None:  # an unclosed ${env:, ${var: or ${secret:
                return self.malformed(m.group(0), pointer)
            return self.expression(m, pointer)

        out: list[str] = []
        pos = size = 0
        for m in _TOKEN.finditer(value):
            out.append(value[pos : m.start()])
            size += m.start() - pos
            pos = m.end()
            piece = repl(m)
            if (m.group(1) or "").startswith("secret:") and piece == m.group(0):
                self.records.append(
                    InterpolationRecord(
                        pointer,
                        self.doc.path,
                        "secret",
                        m.group(1)[7:],
                        False,
                        (size, size + len(piece)),
                    )
                )
            out.append(piece)
            size += len(piece)
        out.append(value[pos:])
        return "".join(out)

    def expression(self, m: re.Match[str], pointer: str) -> str:
        inner, original = m.group(1), m.group(0)
        if inner.startswith("env:"):
            parsed = _ENV.match(inner)
            if not parsed:
                return self.malformed(original, pointer)
            name, default = parsed.group(1), parsed.group(2)
            if name in self.env:
                self.records.append(InterpolationRecord(pointer, self.doc.path, "env", name, False))
                return self.env[name]
            if default is not None:
                self.records.append(InterpolationRecord(pointer, self.doc.path, "env", name, True))
                return default
            self.report.add(
                "PLS106",
                f"environment variable '{name}' is not set",
                location=self.doc.locate(pointer),
                hint=f"set it, or write ${{env:{name}:-default}} to give a default",
                context={"name": name, "kind": "env"},
            )
            return original
        if inner.startswith("var:"):
            parsed = _VAR.match(inner)
            if not parsed:
                return self.malformed(original, pointer)
            name = parsed.group(1)
            if name in self.vars:
                self.records.append(InterpolationRecord(pointer, self.doc.path, "var", name, False))
                return str(self.vars[name])
            self.report.add(
                "PLS106",
                f"unknown variable '{name}'",
                location=self.doc.locate(pointer),
                hint=suggest(name, self.vars) or "declare it under vars or pass it with --vars",
                context={"name": name, "kind": "var"},
            )
            return original
        return original

    def malformed(self, original: str, pointer: str) -> str:
        self.report.add(
            "PLS107",
            f"malformed interpolation '{original}'",
            location=self.doc.locate(pointer),
            hint="use ${env:NAME}, ${env:NAME:-default} or ${var:name}",
        )
        return original


def interpolate(
    doc: LoadedDocument,
    *,
    env: Mapping[str, str],
    vars: Mapping[str, Any],
    report: Report,
    skip: Callable[[str], bool],
) -> tuple[Any, list[InterpolationRecord]]:
    """Return a copy of the document data with interpolations resolved, and what was resolved.

    `skip(pointer)` is asked for every string value; a true answer leaves that value untouched.
    """
    worker = _Interpolator(doc, env, vars, report, skip)
    return worker.walk(doc.data, ""), worker.records
