"""Discovery results as diagnostics and resolver input (ADR-0050 points 1, 2, 11 and 12).

Failures become PLY005 (the module cannot be loaded), PLS101 (unknown keyword) or PLS102
(invalid argument). Each discovered model becomes a `PythonModel`: its `Var` markers resolved
from the project vars and `--vars`, its typed values dumped to plain data, and the result
validated with pydantic, whose errors are located through a `DecoratorSite`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any

from pydantic import BaseModel, ValidationError

from pipeloom.authoring import NO_DEFAULT, Ref, SourceRef, Var
from pipeloom.config.decorator import DecoratorSite, PythonModel
from pipeloom.config.errors import from_validation_error
from pipeloom.config.models import DecoratorArgs, ModelSettings
from pipeloom.diagnostics import Location, Report
from pipeloom.discovery.model import DiscoveredModel, DiscoveryResult

_SETTING_FIELDS = frozenset(ModelSettings.model_fields)

_PLY005_HINTS = {
    "outside_roots": "add the folder that holds it to python_paths",
    "name_mismatch": "rename the module, or remove what shadows it",
}


@dataclass(frozen=True)
class PythonModels:
    models: tuple[PythonModel, ...] = ()
    broken: frozenset[str] = frozenset()
    """Models whose arguments are invalid. Their names stay known to reference checks, so
    dependents report no PLR201 on top of the PLS code."""
    incomplete: bool = False
    """Some model module could not be loaded (PLY005)."""


def python_models(
    result: DiscoveryResult, vars: Mapping[str, Any], report: Report
) -> PythonModels:
    """Report the discovery failures and validate the discovered models."""
    incomplete = False
    broken: set[str] = set()
    for f in result.failures:
        loc = Location(f.file, f.line, f.col)
        if f.kind == "unknown_keyword":
            message, _, hint = f.message.partition("; ")
            report.add("PLS101", message, location=loc, hint=hint or None)
        elif f.kind == "invalid_argument":
            report.add("PLS102", f.message, location=loc)
            if f.model is not None:
                broken.add(f.model)
        else:
            incomplete = True
            report.add(
                "PLY005",
                f.message,
                location=loc,
                hint=_PLY005_HINTS.get(f.kind),
                context={"kind": f.kind},
            )
    models: list[PythonModel] = []
    for d in result.models:
        m = _convert(d, vars, report)
        if m is None:
            broken.add(d.name)
        else:
            models.append(m)
    return PythonModels(tuple(models), frozenset(broken), incomplete)


def _convert(d: DiscoveredModel, vars: Mapping[str, Any], report: Report) -> PythonModel | None:
    site = DecoratorSite(d.file, d.line, d.col, d.kwarg_locations)
    data: dict[str, Any] = {}
    ok = True
    for key, value in d.kwargs.items():
        missing: list[str] = []
        plain = _plain(value, vars, missing)
        for name in missing:
            ok = False
            report.add(
                "PLS106",
                f"unknown variable '{name}'",
                location=site.locate(f"/{key}"),
                hint="define it under vars in pipeloom.yml, pass --vars, or give var() a default",
            )
        if plain is not None:
            data[key] = plain
    if not ok:
        return None
    site = replace(site, data=data)
    try:
        args = DecoratorArgs.model_validate(data)
    except ValidationError as err:
        from_validation_error(err, site, DecoratorArgs, report)
        return None
    settings = args.model_dump(
        include=set(_SETTING_FIELDS), exclude_unset=True, by_alias=True, mode="python"
    )
    if args.description is not None:
        settings["description"] = args.description
    return PythonModel(
        name=d.name,
        callable=d.callable_ref,
        module=d.module,
        site=site,
        settings=settings,
        columns=args.columns,
        inputs=args.inputs,
        params=d.params,
        own_fields=frozenset(data),
    )


def _plain(value: Any, vars: Mapping[str, Any], missing: list[str]) -> Any:
    """Decorator values as YAML-shaped data: vars resolved, typed objects dumped."""
    if isinstance(value, Var):
        if value.name in vars:
            return vars[value.name]
        if value.default is not NO_DEFAULT:
            return value.default
        missing.append(value.name)
        return None
    if isinstance(value, Ref):
        return {"ref": value.name}
    if isinstance(value, SourceRef):
        return {"source": f"{value.group}.{value.table}"}
    if isinstance(value, BaseModel):
        value = value.model_dump(exclude_unset=True, by_alias=True)
    if isinstance(value, Mapping):
        return {k: _plain(v, vars, missing) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [_plain(v, vars, missing) for v in value]
    return value
