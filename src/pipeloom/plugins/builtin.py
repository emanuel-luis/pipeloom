"""The built-in plugins as a lazy table of `module:attr` strings. Nothing here is imported."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from pipeloom.plugins import Group

BUILTINS: Final[Mapping[Group, Mapping[str, str]]] = {
    "catalogs": {
        "filesystem": "pipeloom.builtins.filesystem:SPEC",
        "memory": "pipeloom.testing.memory:CATALOG_SPEC",
    },
    "engines": {"memory": "pipeloom.testing.memory:SPEC"},
    "naming": {
        "none": "pipeloom.builtins.naming:NONE_SPEC",
        "per_user": "pipeloom.builtins.naming:PER_USER_SPEC",
        "prefix": "pipeloom.builtins.naming:PREFIX_SPEC",
    },
    "secret_providers": {"env": "pipeloom.builtins.secrets_env:SPEC"},
}
