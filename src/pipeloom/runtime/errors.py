"""Turn an exception raised by a model, an engine or a catalog into an `ErrorInfo`."""

from __future__ import annotations

import traceback
from pathlib import Path
from typing import Literal

from pipeloom.diagnostics import Diagnostic, Location, Severity
from pipeloom.engine.protocols import EngineSession
from pipeloom.events.model import ErrorInfo
from pipeloom.runtime import retry
from pipeloom.runtime.retry import PERMANENT_CODES, Phase

__all__ = ["ErrorInfo", "Phase", "build_error", "classify", "to_diagnostic"]


def classify(
    exc: BaseException, phase: Phase, session: EngineSession | None = None
) -> Literal["transient", "permanent"]:
    return retry.classify(exc, phase=phase, session=session)


def _scrub(text: str, project_dir: Path) -> str:
    """Show paths below the project dir as relative ones, so messages do not leak the machine."""
    forms = {project_dir.as_posix(), str(project_dir)}
    resolved = project_dir.resolve()
    forms |= {resolved.as_posix(), str(resolved)}
    for form in sorted(forms - {"."}, key=len, reverse=True):
        text = text.replace(form + "/", "").replace(form + "\\", "").replace(form, ".")
    return text


def _root_cause(exc: BaseException) -> BaseException:
    seen = {id(exc)}
    while True:
        nxt = exc.__cause__ or (None if exc.__suppress_context__ else exc.__context__)
        if nxt is None or id(nxt) in seen:
            return exc
        seen.add(id(nxt))
        exc = nxt


def _relative(filename: str, project_dir: Path) -> str | None:
    try:
        return Path(filename).resolve().relative_to(project_dir.resolve()).as_posix()
    except (ValueError, OSError):
        return None


def _type_name(exc: BaseException) -> str:
    t = type(exc)
    return t.__qualname__ if t.__module__ == "builtins" else f"{t.__module__}.{t.__qualname__}"


def build_error(
    code: str,
    exc: BaseException,
    *,
    phase: Phase,
    message: str,
    hint: str | None,
    project_dir: Path,
    fallback: Location | None,
    session: EngineSession | None = None,
) -> ErrorInfo:
    """`message` states what the unit was doing; the exception text is appended to it.

    The excerpt holds the frames under the project dir and the root cause. The location is the
    innermost project frame, or `fallback` (the model's own place in its YAML file).
    """
    root = _root_cause(exc)
    excerpt: list[str] = []
    location = fallback
    for frame in traceback.extract_tb(exc.__traceback__):
        rel = _relative(frame.filename, project_dir)
        if rel is not None:
            excerpt.append(f"{rel}:{frame.lineno} in {frame.name}")
            location = Location(rel, frame.lineno)
    excerpt.append(_scrub(f"{_type_name(root)}: {root}", project_dir))
    detail = _scrub(f"{_type_name(exc)}: {exc}", project_dir)
    return ErrorInfo(
        code=code,
        category="permanent" if code in PERMANENT_CODES else classify(exc, phase, session),
        message=f"{message}: {detail}",
        hint=hint,
        exception_type=_type_name(exc),
        trace_excerpt=excerpt,
        location=None if location is None else location.to_json(),
    )


def full_traceback(exc: BaseException, project_dir: Path) -> str:
    return _scrub("".join(traceback.format_exception(exc)), project_dir)


def to_diagnostic(error: ErrorInfo, *, unit_id: str) -> Diagnostic:
    return Diagnostic(
        code=error.code,
        severity=Severity.ERROR,
        message=error.message,
        hint=error.hint,
        location=None if error.location is None else Location.from_json(error.location),
        context={"unit": unit_id, "category": error.category},
    )
