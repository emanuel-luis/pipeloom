"""Map pydantic validation errors to diagnostics with file locations."""

from __future__ import annotations

import types
import typing
from typing import Any, Union

from pydantic import BaseModel, ValidationError

from pipeloom.config.yaml import LoadedDocument
from pipeloom.diagnostics import Report, suggest


def _escape(segment: str) -> str:
    return segment.replace("~", "~0").replace("/", "~1")


def _resolve(annotation: Any) -> tuple[type[BaseModel] | None, bool]:
    """Find the model inside an annotation; the flag says it sits behind a dict key."""
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Annotated:
        return _resolve(args[0])
    if origin is Union or origin is types.UnionType:
        for arg in args:
            found = _resolve(arg)
            if found[0] is not None:
                return found
        return None, False
    if origin in (list, tuple, set, frozenset) and args:
        return _resolve(args[0])
    if origin is dict and len(args) == 2:
        found = _resolve(args[1])
        return found[0], found[0] is not None
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation, False
    return None, False


def _field_annotation(model: type[BaseModel], name: str) -> tuple[bool, Any]:
    for fname, info in model.model_fields.items():
        if name in (info.alias or fname, fname):
            return True, info.annotation
    return False, None


def _field_names(model: type[BaseModel]) -> list[str]:
    return [info.alias or name for name, info in model.model_fields.items()]


def _get(data: Any, pointer: str) -> Any:
    node = data
    for raw in pointer.split("/")[1:]:
        seg = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(node, dict):
            node = node.get(seg)
        elif isinstance(node, list) and seg.isdigit() and int(seg) < len(node):
            node = node[int(seg)]
        else:
            return None
    return node


def from_validation_error(
    err: ValidationError,
    doc: LoadedDocument,
    root_model: type[BaseModel],
    report: Report,
    *,
    base_pointer: str = "",
) -> None:
    """Add one diagnostic per pydantic error; PLS101, PLS105, PLS108 or PLS102."""
    seen: set[tuple[str, str, str]] = set()
    for item in err.errors(include_url=False, include_context=False, include_input=False):
        node = _get(doc.data, base_pointer)
        pointer = base_pointer
        model: type[BaseModel] | None = root_model
        keyed = False
        before: type[BaseModel] | None = model
        loc = list(item["loc"])
        for seg in loc:
            before = model
            if isinstance(seg, str) and isinstance(node, dict) and seg in node:
                pointer += "/" + _escape(seg)
                node = node[seg]
                if keyed:
                    keyed = False
                elif model is not None:
                    found, ann = _field_annotation(model, seg)
                    model, keyed = _resolve(ann) if found else (None, False)
            elif isinstance(seg, int) and isinstance(node, list) and seg < len(node):
                pointer += f"/{seg}"
                node = node[seg]
        kind = item["type"]
        last = loc[-1] if loc else ""
        if kind == "extra_forbidden":
            hint = suggest(str(last), _field_names(before)) if before is not None else None
            code, message, location = (
                "PLS101",
                f"unknown field '{last}'",
                doc.locate(pointer, key=True),
            )
        elif kind == "missing":
            code, message, location = (
                "PLS105",
                f"missing required field '{last}'",
                doc.locate(pointer),
            )
            hint = None
        elif kind == "pipeloom_exclusive":
            code, message, location = "PLS108", item["msg"], doc.locate(pointer)
            hint = None
        else:
            msg = item["msg"].removeprefix("Value error, ")
            message = f"invalid value for '{last}': {msg}" if isinstance(last, str) else msg
            code, location = "PLS102", doc.locate(pointer)
            hint = None
        key = (code, pointer, message)
        if key in seen:
            continue
        seen.add(key)
        report.add(code, message, location=location, hint=hint, context={"pydantic_type": kind})
