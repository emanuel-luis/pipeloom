"""Import the Python callables of models, with `python_paths` on `sys.path` for the run."""

from __future__ import annotations

import importlib
import sys
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, Self


class CallableLoader:
    """A context manager. `sys.path` is process-global, so two runs in one process must not overlap.

    On exit it removes the entries it added and forgets the modules it imported from them, so
    the next run in the same process imports fresh code.
    """

    def __init__(self, project_dir: Path, python_paths: Sequence[str]) -> None:
        self._paths = [str((project_dir / p).resolve()) for p in python_paths]
        self._added: list[str] = []
        self._before: frozenset[str] = frozenset()
        self._cache: dict[str, Callable[..., Any]] = {}
        self._lock = threading.Lock()

    def __enter__(self) -> Self:
        self._before = frozenset(sys.modules)
        for p in reversed(self._paths):  # keep the configured order at the front
            sys.path.insert(0, p)
            self._added.append(p)
        importlib.invalidate_caches()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        for p in self._added:
            if p in sys.path:
                sys.path.remove(p)
        roots = tuple(self._paths)
        for name in set(sys.modules) - self._before:
            file = getattr(sys.modules[name], "__file__", None)
            if file and str(Path(file).resolve()).startswith(roots):
                del sys.modules[name]
        self._added.clear()
        self._cache.clear()

    def load(self, ref: str) -> Callable[..., Any]:
        """The function named by `module:function`; imported once."""
        with self._lock:
            found = self._cache.get(ref)
            if found is None:
                module, _, attr = ref.partition(":")
                obj = getattr(importlib.import_module(module), attr)
                if not callable(obj):
                    raise TypeError(f"'{ref}' is not callable")
                found = self._cache[ref] = obj
            return found
