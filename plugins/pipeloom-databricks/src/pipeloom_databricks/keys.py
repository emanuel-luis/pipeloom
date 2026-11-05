"""Task keys: 1 to 100 characters of letters, digits, `_` and `-`."""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from collections.abc import Sequence
from typing import Final

FINALIZE_KEY: Final = "pipeloom__finalize"
MAX_KEY_LEN: Final = 100
MAX_TASKS: Final = 1000
"""Read as `keys.MAX_TASKS` at call time, so a test can lower it."""

_UNSAFE = re.compile(r"[^A-Za-z0-9_-]")
_HASH_LEN = 8
_KEEP = MAX_KEY_LEN - _HASH_LEN - 1


def sanitize(raw: str) -> str:
    """Replace every character outside `[A-Za-z0-9_-]` with `_`. Never empty."""
    return _UNSAFE.sub("_", raw) or "_"


def _hashed(key: str, raw: str) -> str:
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:_HASH_LEN]
    return f"{key[:_KEEP]}_{digest}"


def assign_keys(raw_ids: Sequence[str]) -> dict[str, str]:
    """A task key for each raw id.

    The key is the sanitized id. One longer than 100 characters, or one that several ids share
    (or that is the finalize key), becomes its first 91 characters, `_` and 8 hex characters of
    the sha256 of the raw id. Every id in a collision gets the suffix, so the result does not
    depend on the order of `raw_ids`.
    """
    base = {raw: sanitize(raw) for raw in raw_ids}
    taken = Counter(base.values())
    taken[FINALIZE_KEY] += 1
    return {
        raw: _hashed(key, raw) if len(key) > MAX_KEY_LEN or taken[key] > 1 else key
        for raw, key in base.items()
    }
