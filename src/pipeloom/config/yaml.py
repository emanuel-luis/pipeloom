"""YAML loading with YAML 1.2 core scalars, source marks and duplicate-key detection.

The parser is PyYAML. Scalars resolve with the YAML 1.2 core schema (only true and false are
booleans, no implicit timestamps), so `country: NO` stays a string. The node tree is walked by
our own constructor, which records the mark of every key and value under its RFC 6901 pointer,
reports duplicate keys, supports merge keys and limits alias expansion.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

from pipeloom.diagnostics import Location, Report

MAX_NODES: Final = 100_000

_TAG = "tag:yaml.org,2002:"
_NULL_RE = re.compile(r"^(?:~|null|Null|NULL|)$")
_BOOL_RE = re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$")
_INT_RE = re.compile(r"^(?:[-+]?[0-9]+|0o[0-7]+|0x[0-9a-fA-F]+)$")
_FLOAT_RE = re.compile(
    r"^(?:[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?"
    r"|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$"
)
_MERGE_RE = re.compile(r"^<<$")


def _core_resolvers() -> dict[str, list[tuple[str, re.Pattern[str]]]]:
    table: dict[str, list[tuple[str, re.Pattern[str]]]] = {}
    for tag, regex, first in (
        (_TAG + "null", _NULL_RE, "~nN"),
        (_TAG + "bool", _BOOL_RE, "tTfF"),
        (_TAG + "int", _INT_RE, "-+0123456789"),
        (_TAG + "float", _FLOAT_RE, "-+0123456789."),
        (_TAG + "merge", _MERGE_RE, "<"),
    ):
        for ch in first:
            table.setdefault(ch, []).append((tag, regex))
    table.setdefault("", []).append((_TAG + "null", _NULL_RE))
    return table


class Yaml12Loader(yaml.SafeLoader):
    """SafeLoader whose implicit resolvers are the YAML 1.2 core schema plus merge keys."""

    yaml_implicit_resolvers = _core_resolvers()


@dataclass(frozen=True)
class Mark:
    line: int
    col: int


def _escape(segment: str) -> str:
    return segment.replace("~", "~0").replace("/", "~1")


@dataclass(frozen=True)
class LoadedDocument:
    path: str
    data: Any
    value_marks: Mapping[str, Mark]
    key_marks: Mapping[str, Mark]

    def locate(self, pointer: str, *, key: bool = False) -> Location:
        """Location of a pointer; falls back to the nearest ancestor that has a mark."""
        current = pointer
        first = True
        while True:
            marks = self.key_marks if key and first else self.value_marks
            mark = marks.get(current) or self.value_marks.get(current)
            if mark is not None:
                return Location(self.path, mark.line, mark.col, pointer)
            if not current:
                return Location(self.path, None, None, pointer)
            current = current.rsplit("/", 1)[0]
            first = False


class _Problem(Exception):
    def __init__(self, code: str, message: str, node: Node | None, hint: str | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.node = node
        self.hint = hint


def _mark_of(node: Node) -> Mark:
    return Mark(node.start_mark.line + 1, node.start_mark.column + 1)


class _Builder:
    def __init__(self, path: str) -> None:
        self.path = path
        self.value_marks: dict[str, Mark] = {}
        self.key_marks: dict[str, Mark] = {}
        self.count = 0
        self.active: set[int] = set()

    def build(self, node: Node, pointer: str) -> Any:
        self.count += 1
        if self.count > MAX_NODES:
            raise _Problem(
                "PLY001",
                f"alias expansion exceeds {MAX_NODES} nodes",
                node,
                "remove nested aliases, or split the data",
            )
        self.value_marks[pointer] = _mark_of(node)
        if isinstance(node, ScalarNode):
            return self.scalar(node)
        if id(node) in self.active:
            raise _Problem("PLY001", "recursive alias", node)
        self.active.add(id(node))
        try:
            if isinstance(node, SequenceNode):
                return [self.build(n, f"{pointer}/{i}") for i, n in enumerate(node.value)]
            if isinstance(node, MappingNode):
                return self.mapping(node, pointer)
        finally:
            self.active.discard(id(node))
        raise _Problem("PLY001", "unsupported YAML node", node)  # pragma: no cover

    def scalar(self, node: ScalarNode) -> Any:
        tag, text = node.tag, node.value
        if tag == _TAG + "str":
            return text
        if tag == _TAG + "null":
            return None
        if tag == _TAG + "bool":
            return text.lower() == "true"
        if tag == _TAG + "int":
            sign = -1 if text.startswith("-") else 1
            body = text.lstrip("+-")
            return sign * int(body, 0 if body[:2] in ("0o", "0x") else 10)
        if tag == _TAG + "float":
            lowered = text.lower().lstrip("+")
            if lowered.endswith(".inf"):
                return float("-inf") if lowered.startswith("-") else float("inf")
            if lowered == ".nan":
                return float("nan")
            return float(text)
        raise _Problem("PLY001", f"unsupported tag '{tag}'", node)

    def key_of(self, node: Node) -> str:
        if not isinstance(node, ScalarNode):
            raise _Problem("PLY001", "mapping keys must be plain scalars", node)
        key = self.scalar(node)
        if not isinstance(key, str):
            raise _Problem(
                "PLY001",
                "mapping keys must be strings",
                node,
                "quote the key to make it a string",
            )
        return key

    def mapping(self, node: MappingNode, pointer: str) -> dict[str, Any]:
        explicit: dict[str, tuple[Node, Node]] = {}
        merged: list[tuple[str, Node, Node]] = []
        for knode, vnode in node.value:
            if isinstance(knode, ScalarNode) and knode.tag == _TAG + "merge":
                for source in self._merge_sources(vnode):
                    self.count += 1
                    for k2, v2 in source.value:
                        merged.append((self.key_of(k2), k2, v2))
                continue
            key = self.key_of(knode)
            if key in explicit:
                first = explicit[key][0]
                raise _Problem(
                    "PLY002",
                    f"duplicate key '{key}'",
                    knode,
                    f"first defined at line {first.start_mark.line + 1}",
                )
            explicit[key] = (knode, vnode)
        entries: dict[str, tuple[Node, Node]] = {}
        for key, k2, v2 in merged:
            entries.setdefault(key, (k2, v2))  # earlier merge source wins
        entries.update(explicit)  # explicit keys win over merged ones
        result: dict[str, Any] = {}
        for key, (knode, vnode) in entries.items():
            child = f"{pointer}/{_escape(key)}"
            self.key_marks[child] = _mark_of(knode)
            result[key] = self.build(vnode, child)
        return result

    def _merge_sources(self, vnode: Node) -> list[MappingNode]:
        if isinstance(vnode, MappingNode):
            return [vnode]
        if isinstance(vnode, SequenceNode) and all(isinstance(n, MappingNode) for n in vnode.value):
            return [n for n in vnode.value if isinstance(n, MappingNode)]
        raise _Problem("PLY001", "a merge key needs a mapping or a list of mappings", vnode)


def _line_of(text: str, index: int) -> tuple[int, int]:
    head = text[:index]
    return head.count("\n") + 1, index - (head.rfind("\n") + 1) + 1


def parse_yaml(data: bytes, path: str, report: Report) -> LoadedDocument | None:
    """Parse one file. Report PLY001 to PLY003 and return None on failure."""
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        line = data.count(b"\n", 0, exc.start) + 1
        report.add(
            "PLY003",
            "file is not valid UTF-8",
            location=Location(path, line, None, None),
            hint="save the file as UTF-8 (UTF-16 is not supported)",
        )
        return None
    loader: Yaml12Loader | None = None
    try:
        loader = Yaml12Loader(text)
        node = loader.get_single_node()
        if node is None:
            return LoadedDocument(path, {}, {"": Mark(1, 1)}, {})
        builder = _Builder(path)
        value = builder.build(node, "")
        return LoadedDocument(path, value, builder.value_marks, builder.key_marks)
    except yaml.reader.ReaderError as exc:
        line, col = _line_of(text, exc.position)
        report.add(
            "PLY003",
            "file contains a character YAML does not allow",
            location=Location(path, line, col, None),
        )
    except _Problem as exc:
        loc = None
        if exc.node is not None:
            mark = _mark_of(exc.node)
            loc = Location(path, mark.line, mark.col, None)
        report.add(exc.code, exc.message, location=loc, hint=exc.hint)
    except yaml.MarkedYAMLError as exc:
        pm = exc.problem_mark
        loc = Location(path, pm.line + 1, pm.column + 1, None) if pm else Location(path)
        message = exc.problem or "invalid YAML"
        if "single document" in str(exc.context):
            message = "more than one YAML document in the file"
        report.add("PLY001", message, location=loc)
    except RecursionError:
        report.add(
            "PLY001", "YAML is nested too deeply", location=Location(path), hint="flatten it"
        )
    finally:
        if loader is not None:
            loader.dispose()
    return None
