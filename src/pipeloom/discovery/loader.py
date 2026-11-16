"""Import model modules with `python_paths` on `sys.path`, and forget them afterwards
(ADR-0050 point 2, import fallback and load summary).

Grown from `pipeloom.runtime.loader.CallableLoader`; the runtime switches to `ModuleLoader.load`
when discovery is wired in.
"""

from __future__ import annotations

import importlib
import os
import sys
import threading
import traceback
from collections.abc import Callable, Collection, Mapping, Sequence
from pathlib import Path
from types import ModuleType, TracebackType
from typing import Any, Self

from pipeloom.authoring import MARKER, ModelSpec
from pipeloom.discovery.ast_resolver import Site
from pipeloom.discovery.files import SourceFile
from pipeloom.discovery.model import DiscoveredModel, DiscoveryFailure, FailureKind, params_of

# `sys.path` and `sys.modules` are process-wide, so two loaders must never overlap. Re-entrant,
# so a thread that holds a loader may open another one (a compile inside a run).
_LOCK = threading.RLock()


_NEVER_PURGED = frozenset({"pipeloom", "__main__", "__mp_main__"})


def _norm(path: str | Path) -> str:
    return os.path.normcase(os.path.abspath(path))


def _same(a: str | Path, b: str | Path) -> bool:
    return _norm(a) == _norm(b)


class ModuleLoader:
    """A context manager that puts the `python_paths` roots first on `sys.path`.

    On enter and on exit it purges every module whose `__file__` or `__path__` lies under the
    roots or `purge_dirs` (namespace packages too), so each load imports fresh code and two
    projects with the same module names never see each other's modules. The interpreter's own
    prefixes and pipeloom itself are never purged, even when a virtualenv sits under a root.
    """

    def __init__(
        self, project_dir: Path, python_paths: Sequence[str], purge_dirs: Sequence[Path] = ()
    ) -> None:
        project_dir = project_dir.resolve()
        self.roots = [(project_dir / p).resolve() for p in python_paths]
        self._purge = tuple(_norm(p) + os.sep for p in [*self.roots, *purge_dirs])
        self._keep = tuple(_norm(p) + os.sep for p in {sys.prefix, sys.base_prefix})
        self._added: list[str] = []
        self._cache: dict[str, Callable[..., Any]] = {}
        self._cache_lock = threading.Lock()

    def __enter__(self) -> Self:
        _LOCK.acquire()
        try:
            self._purge_modules()
            for root in reversed(self.roots):  # keep the configured order at the front
                sys.path.insert(0, str(root))
                self._added.append(str(root))
            importlib.invalidate_caches()
        except BaseException:
            _LOCK.release()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            # Purge first: a namespace package recomputes `__path__` from `sys.path`.
            self._purge_modules()
            for p in self._added:
                if p in sys.path:
                    sys.path.remove(p)
            self._added.clear()
            self._cache.clear()
            importlib.invalidate_caches()
        finally:
            _LOCK.release()

    def _owned(self, module: ModuleType) -> bool:
        try:
            file = getattr(module, "__file__", None)
            paths = [file] if file else list(getattr(module, "__path__", None) or ())
        except Exception:  # noqa: BLE001 - a lazy module may raise on attribute access
            return False
        normed = [_norm(p) for p in paths if isinstance(p, str)]
        return bool(normed) and all(
            p.startswith(self._purge) and not p.startswith(self._keep) for p in normed
        )

    def _purge_modules(self) -> None:
        # Decide for every module before deleting any: a nested namespace package reads its
        # parent's `__path__` from `sys.modules`.
        owned = [
            name
            for name, module in list(sys.modules.items())
            if name not in _NEVER_PURGED
            and not name.startswith("pipeloom.")
            and module is not None
            and self._owned(module)
        ]
        for name in owned:
            sys.modules.pop(name, None)

    def load(self, ref: str) -> Callable[..., Any]:
        """The callable named by `module:attr`; its module is imported once per loader."""
        with self._cache_lock:
            found = self._cache.get(ref)
            if found is None:
                module, _, attr = ref.partition(":")
                obj = getattr(importlib.import_module(module), attr)
                if not callable(obj):
                    raise TypeError(f"'{ref}' is not callable")
                found = self._cache[ref] = obj
            return found

    def import_models(
        self,
        files: Sequence[SourceFile],
        sites: Mapping[str, Mapping[str, Site]],
        known_modules: Collection[str],
    ) -> tuple[list[DiscoveredModel], list[DiscoveryFailure]]:
        """Import each file and collect its marked functions. Never stops at the first failure.

        `sites` holds the AST decorator sites by file path and function name. A function seen in
        several modules is kept where `fn.__module__` names the module, and is left to its own
        module when that is one of `known_modules` (it is discovered there), else kept in the
        first module in path order.
        """
        failures: list[DiscoveryFailure] = []
        found: dict[int, DiscoveredModel] = {}
        for sf in files:
            module = self._import(sf, failures)
            if module is None:
                continue
            for attr, obj in list(vars(module).items()):
                spec = _spec(obj)
                if spec is None:
                    continue
                owner = getattr(obj, "__module__", None)
                if owner != module.__name__ and owner in known_modules:
                    continue
                prev = found.get(id(obj))
                if prev is not None and (prev.module == owner or owner != module.__name__):
                    continue
                found[id(obj)] = _model(
                    sf, module.__name__, attr, obj, spec, sites.get(sf.path, {})
                )
        return list(found.values()), failures

    def _import(self, sf: SourceFile, failures: list[DiscoveryFailure]) -> ModuleType | None:
        def fail(
            kind: FailureKind, line: int | None, col: int | None, msg: str, tb: str | None = None
        ) -> None:
            failures.append(DiscoveryFailure(sf.path, line, col, kind, msg, tb))

        if sf.module is None:
            roots = ", ".join(r.as_posix() for r in self.roots)
            fail(
                "outside_roots",
                None,
                None,
                f"'{sf.path}' is not under any python_paths root ({roots})",
            )
            return None
        try:
            module = importlib.import_module(sf.module)
        except SyntaxError as exc:
            if exc.filename and _same(exc.filename, sf.abs):
                fail("syntax", exc.lineno, exc.offset, exc.msg)
            else:
                fail("syntax", _frame_line(exc, sf.abs), None, _summary(exc), _tail(exc))
            return None
        except (KeyboardInterrupt, GeneratorExit):
            raise
        except BaseException as exc:  # noqa: BLE001 - user code may raise anything, SystemExit too
            fail("import_error", _frame_line(exc, sf.abs), None, _summary(exc), _tail(exc))
            return None
        file = getattr(module, "__file__", None)
        if not file or not _same(file, sf.abs):
            where = Path(file).as_posix() if file else "a namespace package"
            fail(
                "name_mismatch",
                None,
                None,
                f"module '{sf.module}' resolves to {where}, not '{sf.path}': another module "
                "or installed package with that name shadows it",
            )
            return None
        return module


def _spec(obj: object) -> ModelSpec | None:
    try:
        spec = getattr(obj, MARKER, None)
    except Exception:  # noqa: BLE001 - an object whose __getattr__ raises
        return None
    return spec if isinstance(spec, ModelSpec) else None


def _model(
    sf: SourceFile,
    module: str,
    attr: str,
    fn: Any,
    spec: ModelSpec,
    sites: Mapping[str, Site],
) -> DiscoveredModel:
    kwargs = dict(spec.kwargs)
    code = getattr(fn, "__code__", None)
    fn_name = getattr(fn, "__name__", attr)
    site = sites.get(fn_name)
    if site is None or code is None or not _same(code.co_filename, sf.abs):
        line, col, locs = (code.co_firstlineno if code else 1), 1, {}
    else:
        line, col = site.line, site.col
        locs = {k: v for k, v in site.kwarg_locations.items() if k in kwargs}
    try:
        params = params_of(fn)
    except (TypeError, ValueError):
        params = ()
    name = kwargs.get("name")
    return DiscoveredModel(
        name=name if isinstance(name, str) else fn_name,
        module=module,
        attr=attr,
        file=sf.path,
        line=line,
        col=col,
        kwargs=kwargs,
        kwarg_locations=locs,
        params=params,
        dynamic=True,
    )


def _frame_line(exc: BaseException, file: Path) -> int | None:
    """The line of the innermost traceback frame inside `file`."""
    for frame in reversed(traceback.extract_tb(exc.__traceback__)):
        if _same(frame.filename, file):
            return frame.lineno
    return None


def _summary(exc: BaseException) -> str:
    return "".join(traceback.format_exception_only(exc)).strip()


def _tail(exc: BaseException) -> str:
    """The last three frames, the most relevant ones for a load summary."""
    return "".join(traceback.format_list(traceback.extract_tb(exc.__traceback__)[-3:])).rstrip()
