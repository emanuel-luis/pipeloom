"""Plugin discovery and the plugin API gate.

Discovery reads entry-point metadata and the built-in table only. A plugin module is imported
when a config names its kind, when `--plugin` names it, or when a check asks for it.

Spec modules must not import heavy libraries at module level, so the core can read a spec's
capabilities without importing an engine.
"""

from __future__ import annotations

import importlib
import importlib.metadata
import os
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final, Generic, Literal, TypeVar, get_args

from pydantic import BaseModel, ConfigDict

from pipeloom.diagnostics import Location, Report
from pipeloom.engine.types import CatalogCapabilities, EngineCapabilities, StateStoreCapabilities

API_VERSION: Final = (0, 5)

Group = Literal[
    "engines",
    "catalogs",
    "state_stores",
    "sinks",
    "secret_providers",
    "executors",
    "renderers",
    "naming",
    "commands",
]
GROUPS: Final[tuple[Group, ...]] = tuple(g for g in get_args(Group) if g != "commands")
"""The groups whose entries are PluginSpecs. `commands` (the CLI) has no spec: see cli/_ext.py."""
DISABLE_AUTOLOAD_ENV: Final = "PIPELOOM_DISABLE_PLUGIN_AUTOLOAD"


def autoload_enabled(env: Mapping[str, str]) -> bool:
    """False when PIPELOOM_DISABLE_PLUGIN_AUTOLOAD is 1, true or yes."""
    return env.get(DISABLE_AUTOLOAD_ENV, "").lower() not in ("1", "true", "yes")


T = TypeVar("T")

__all__ = [
    "API_VERSION",
    "GROUPS",
    "Group",
    "OptionsModel",
    "PluginEntry",
    "PluginRegistry",
    "PluginSpec",
    "autoload_enabled",
    "check_api",
]


class OptionsModel(BaseModel):
    """Base for a plugin's `config_model`: frozen, no unknown options, docstrings as help."""

    model_config = ConfigDict(
        frozen=True, extra="forbid", use_attribute_docstrings=True, protected_namespaces=()
    )


@dataclass(frozen=True)
class PluginSpec(Generic[T]):
    name: str
    kind: str
    group: Group
    api_version: tuple[int, int]
    factory: Callable[[Any], T]
    """Builds the plugin from an instance of `config_model`."""
    config_model: type[BaseModel]
    """The plugin's options, without `kind`."""
    compatible_engines: frozenset[str] = frozenset()
    """Engine kinds a catalog works with; empty means any."""
    capabilities: EngineCapabilities | StateStoreCapabilities | CatalogCapabilities | None = None
    """Static data for L5, readable without building the plugin."""
    description: str = ""


@dataclass(frozen=True)
class PluginEntry:
    group: Group
    kind: str
    value: str
    """`module:attr` that resolves to a PluginSpec."""
    distribution: str
    version: str
    source: Literal["builtin", "entry_point", "cli"]


def check_api(spec: PluginSpec[Any]) -> str | None:
    """None when the spec fits this core, else the reason.

    While the major version is 0 the versions must match exactly. From 1.0 the major must match
    and the minor must not exceed the core's.
    """
    core, got = API_VERSION, spec.api_version
    fits = got == core if core[0] == 0 else got[0] == core[0] and got[1] <= core[1]
    if fits:
        return None
    return (
        f"plugin '{spec.kind}' targets plugin API {got[0]}.{got[1]}, "
        f"but this pipeloom provides {core[0]}.{core[1]}"
    )


def _import_ref(value: str) -> Any:
    module, sep, attr = value.partition(":")
    if not sep or not module or not attr:
        raise ValueError(f"'{value}' is not in the form module:attr")
    obj: Any = importlib.import_module(module)
    for part in attr.split("."):
        obj = getattr(obj, part)
    return obj


def _sort_key(e: PluginEntry) -> tuple[str, str, str, str]:
    return (e.group, e.kind, e.source, e.value)


class PluginRegistry:
    """Known plugins by (group, kind). Build one with `discover`."""

    def __init__(
        self,
        entries: Sequence[PluginEntry],
        imported: Mapping[tuple[Group, str, str], PluginSpec[Any]] | None = None,
    ) -> None:
        self._entries = tuple(sorted(entries, key=_sort_key))
        # Specs already imported, keyed by (group, kind, value). `--plugin` fills it up front.
        self._specs: dict[tuple[Group, str, str], PluginSpec[Any]] = dict(imported or {})

    @classmethod
    def discover(
        cls,
        *,
        cli_plugins: Sequence[str] = (),
        autoload: bool | None = None,
        env: Mapping[str, str] | None = None,
        report: Report | None = None,
    ) -> PluginRegistry:
        """Collect built-ins, entry points (unless autoload is off) and `--plugin` specs.

        Entry points and built-ins are not imported. `--plugin` modules are, because the group
        and kind are only known once the spec is read; failures go to `report` as PLU003.
        """
        from pipeloom import __version__
        from pipeloom.plugins.builtin import BUILTINS

        env = os.environ if env is None else env
        if autoload is None:
            autoload = autoload_enabled(env)
        report = report if report is not None else Report()

        entries: dict[tuple[str, str, str, str], PluginEntry] = {}
        for group, table in BUILTINS.items():
            for kind, value in table.items():
                entries[("pipeloom", kind, value, group)] = PluginEntry(
                    group, kind, value, "pipeloom", __version__, "builtin"
                )
        if autoload:
            for group in GROUPS:
                for ep in importlib.metadata.entry_points(group=f"pipeloom.{group}"):
                    dist = getattr(ep, "dist", None)
                    name = getattr(dist, "name", None) or "unknown"
                    key = (name, ep.name, ep.value, group)
                    entries.setdefault(
                        key,
                        PluginEntry(
                            group,
                            ep.name,
                            ep.value,
                            name,
                            getattr(dist, "version", None) or "",
                            "entry_point",
                        ),
                    )
        imported: dict[tuple[Group, str, str], PluginSpec[Any]] = {}
        for value in dict.fromkeys(cli_plugins):
            spec = cls._import_cli(value, report)
            if spec is None:
                continue
            entries[("(cli)", spec.kind, value, spec.group)] = PluginEntry(
                spec.group, spec.kind, value, "(cli)", "", "cli"
            )
            imported[(spec.group, spec.kind, value)] = spec
        return cls(list(entries.values()), imported)

    @staticmethod
    def _import_cli(value: str, report: Report) -> PluginSpec[Any] | None:
        try:
            obj = _import_ref(value)
        except Exception as exc:  # noqa: BLE001 - any import-time failure of user code
            report.add("PLU003", f"cannot load --plugin '{value}': {type(exc).__name__}: {exc}")
            return None
        if not isinstance(obj, PluginSpec):
            report.add("PLU003", f"--plugin '{value}' is not a PluginSpec")
            return None
        return obj

    def entries(self, group: Group | None = None) -> tuple[PluginEntry, ...]:
        return tuple(e for e in self._entries if group is None or e.group == group)

    def duplicates(self) -> Mapping[tuple[Group, str], tuple[PluginEntry, ...]]:
        by_key: dict[tuple[Group, str], list[PluginEntry]] = defaultdict(list)
        for e in self._entries:
            by_key[(e.group, e.kind)].append(e)
        return {k: tuple(v) for k, v in by_key.items() if len(v) > 1}

    def load(
        self, group: Group, kind: str, report: Report, location: Location | None = None
    ) -> PluginSpec[Any] | None:
        """Import and check the plugin for (group, kind). None after reporting PLU001-PLU003.

        An unknown kind returns None without a diagnostic; the caller reports PLR204.
        """
        matches = [e for e in self._entries if e.group == group and e.kind == kind]
        if not matches:
            return None
        if len(matches) > 1:
            where = ", ".join(f"{e.distribution} ({e.value})" for e in matches)
            report.add(
                "PLU002",
                f"{group} kind '{kind}' is provided more than once: {where}",
                location=location,
                hint="uninstall one distribution or set PIPELOOM_DISABLE_PLUGIN_AUTOLOAD=1",
                context={"group": group, "kind": kind},
            )
            return None
        return self.check(matches[0], report, location)

    def check(
        self, entry: PluginEntry, report: Report, location: Location | None = None
    ) -> PluginSpec[Any] | None:
        """Import one entry (once) and verify it is a spec for its group and kind, on this API."""
        key = (entry.group, entry.kind, entry.value)
        spec = self._specs.get(key)
        ctx: dict[str, Any] = {"group": entry.group, "kind": entry.kind, "value": entry.value}
        if spec is None:
            try:
                obj = _import_ref(entry.value)
            except Exception as exc:  # noqa: BLE001 - any import-time failure of plugin code
                report.add(
                    "PLU003",
                    f"cannot load {entry.group} plugin '{entry.kind}' from {entry.value}: "
                    f"{type(exc).__name__}: {exc}",
                    location=location,
                    context=ctx,
                )
                return None
            if not isinstance(obj, PluginSpec):
                report.add(
                    "PLU003",
                    f"{entry.value} for {entry.group} '{entry.kind}' is not a PluginSpec",
                    location=location,
                    context=ctx,
                )
                return None
            spec = obj
        if spec.group != entry.group or spec.kind != entry.kind:
            report.add(
                "PLU003",
                f"{entry.value} declares {spec.group} '{spec.kind}' "
                f"but is registered as {entry.group} '{entry.kind}'",
                location=location,
                context=ctx,
            )
            return None
        problem = check_api(spec)
        if problem is not None:
            report.add(
                "PLU001",
                problem,
                location=location,
                hint=f"install a version of {entry.distribution} built for this pipeloom",
                context={
                    **ctx,
                    "plugin_api": list(spec.api_version),
                    "core_api": list(API_VERSION),
                },
            )
            return None
        self._specs[key] = spec
        return spec

    def load_all(
        self, report: Report, group: Group | None = None
    ) -> dict[Group, list[PluginSpec[Any]]]:
        """Import every entry that is not a duplicate; each duplicate kind is PLU002.

        Used by schema export, which must not drop a kind silently.
        """
        dupes = self.duplicates()
        for (grp, kind), found in dupes.items():
            if group is None or grp == group:
                where = ", ".join(f"{e.distribution} ({e.value})" for e in found)
                report.add(
                    "PLU002",
                    f"{grp} kind '{kind}' is provided more than once: {where}",
                    context={"group": grp, "kind": kind},
                )
        out: dict[Group, list[PluginSpec[Any]]] = {}
        for e in self.entries(group):
            if (e.group, e.kind) in dupes:
                continue
            spec = self.check(e, report)
            if spec is not None:
                out.setdefault(e.group, []).append(spec)
        return out
