"""Naming strategies: where a model is written."""

from __future__ import annotations

import re
from dataclasses import dataclass

from pydantic import Field

from pipeloom.engine.protocols import NamingStrategy
from pipeloom.engine.types import TargetRef
from pipeloom.platform import Identity
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec


class NoneOptions(OptionsModel):
    catalog: str | None = None
    """Catalog of every relation."""
    schema_: str = Field(default="main", alias="schema")
    """Default schema; a model's own schema wins."""


class PrefixOptions(NoneOptions):
    schema_prefix: str = ""
    """Text put in front of the schema name."""


class PerUserOptions(NoneOptions):
    """Schema becomes `<schema>_<user>`, so each developer writes to their own schema."""


def _user_suffix(user: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", user.lower())


@dataclass(frozen=True)
class _Naming:
    catalog: str | None
    schema: str
    prefix: str = ""
    per_user: bool = False

    def relation(self, name: str, schema: str | None, identity: Identity) -> TargetRef:
        base = f"{self.prefix}{schema or self.schema}"
        if self.per_user:
            base = f"{base}_{_user_suffix(identity.user)}"
        return TargetRef(self.catalog, base, name)


def _none(opts: NoneOptions) -> NamingStrategy:
    return _Naming(opts.catalog, opts.schema_)


def _prefix(opts: PrefixOptions) -> NamingStrategy:
    return _Naming(opts.catalog, opts.schema_, prefix=opts.schema_prefix)


def _per_user(opts: PerUserOptions) -> NamingStrategy:
    return _Naming(opts.catalog, opts.schema_, per_user=True)


NONE_SPEC: PluginSpec[NamingStrategy] = PluginSpec(
    "none", "none", "naming", API_VERSION, _none, NoneOptions, description="Schema as given."
)
PREFIX_SPEC: PluginSpec[NamingStrategy] = PluginSpec(
    "prefix",
    "prefix",
    "naming",
    API_VERSION,
    _prefix,
    PrefixOptions,
    description="Schema with a fixed prefix.",
)
PER_USER_SPEC: PluginSpec[NamingStrategy] = PluginSpec(
    "per_user",
    "per_user",
    "naming",
    API_VERSION,
    _per_user,
    PerUserOptions,
    description="Schema with the user name appended.",
)
