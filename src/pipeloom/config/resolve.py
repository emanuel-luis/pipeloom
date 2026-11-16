"""Resolve a bound project: apply settings to every model and give models and sources ids.

Settings precedence, highest first:

1. the model's `@model` decorator;
2. its properties entry: `config`, plus `description` and `columns`;
3. for each folder from the model's own up to its model_paths root, the `models:` tree entry
   for that folder;
4. the root `+key` settings of the tree;
5. the built-in defaults of `ResolvedModelConfig`.

Mappings merge deeply, lists replace, and `tags` is an ordered union without duplicates. An
explicit `null` removes a scalar or mapping set by a lower layer; `tags: null` is ignored.
Decorator `columns` replace the properties `columns` whole.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, NamedTuple

from pipeloom.config.bind import BoundProject
from pipeloom.config.decorator import POINTER, DecoratorSite, PythonModel
from pipeloom.config.discover import PROJECT_FILE
from pipeloom.config.models import (
    InputConfig,
    ModelConfig,
    ModelSettings,
    PropertiesEntry,
    ResolvedModelConfig,
    SourceConfig,
    UnitTestConfig,
    WriteConfig,
)
from pipeloom.config.yaml import LoadedDocument
from pipeloom.diagnostics import Location, suggest
from pipeloom.secrets.interpolation import InterpolationRecord

_SETTING_FIELDS = frozenset(ModelSettings.model_fields)

Layer = Literal["decorator", "properties", "folder", "project"]


def model_id(project: str, name: str) -> str:
    return f"model.{project}.{name}"


def source_id(project: str, name: str) -> str:
    return f"source.{project}.{name}"


@dataclass(frozen=True)
class InputBinding:
    kind: Literal["model", "source"]
    unique_id: str

    @property
    def name(self) -> str:
        """The referenced model or source name (ids are `kind.project.name`)."""
        return self.unique_id.split(".", 2)[2]


@dataclass(frozen=True)
class ResolvedSource:
    unique_id: str
    name: str
    """The source name, or `group.table` for a table of a source group."""
    file: str
    pointer: str
    """JSON pointer of the table in pipeloom.yml, for example `/sources/raw/tables/orders`."""
    config: SourceConfig


class Origin(NamedTuple):
    """The layer a setting came from, and where that layer sets it."""

    layer: Layer
    location: Location


@dataclass(frozen=True)
class ResolvedModel:
    unique_id: str
    name: str
    file: str
    pointer: str
    """JSON pointer of the decorator (`/@model`)."""
    folder: str
    entry: ModelConfig
    """The model entry; its `columns` come from the properties entry when it sets none."""
    config: ResolvedModelConfig
    inputs: Mapping[str, InputBinding]
    own_fields: frozenset[str]
    """Keywords the decorator sets itself."""
    site: DecoratorSite
    """The `@model` decorator."""
    origins: Mapping[str, Origin] = field(default_factory=dict)
    """The winning layer of each setting, `description` and `columns` that some layer sets."""
    data_tests_at: tuple[str, str] | None = None
    """File and JSON pointer of the properties `data_tests` list; None when the decorator sets
    them or no layer does."""


@dataclass(frozen=True)
class Problem:
    """A finding made while resolving, reported by L2 (PLR201, PLR210) or L6 (PLK602)."""

    code: str
    message: str
    location: Location
    hint: str | None = None
    context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PropertiesProblem:
    """A properties entry that names no model, or a model another entry already describes."""

    name: str
    file: str
    pointer: str
    first: Location | None
    """The entry that already describes the model, when there is one."""


@dataclass(frozen=True)
class ResolvedUnitTest:
    model: str
    """Name of the tested model."""
    name: str
    file: str
    pointer: str
    """JSON pointer of the case in its properties file, for example `/models/0/unit_tests/1`."""
    config: UnitTestConfig


@dataclass(frozen=True)
class ResolvedProject:
    bound: BoundProject
    models: Mapping[str, ResolvedModel]
    """Keyed by unique id, in file order. The first definition of a name wins."""
    sources: Mapping[str, ResolvedSource]
    shadowed_models: tuple[ResolvedModel, ...]
    """Later definitions of a model name already taken (PLR202)."""
    docs: Mapping[str, LoadedDocument]
    interpolations: tuple[InterpolationRecord, ...]
    unit_tests: tuple[ResolvedUnitTest, ...] = ()
    """Every unit test, in file order (ADR-0045)."""
    properties_problems: tuple[PropertiesProblem, ...] = ()
    """Properties entries that describe no model or a model twice (PLR209)."""
    binding_problems: tuple[Problem, ...] = ()
    """Parameters of Python-first models that bind to nothing, or ambiguously."""
    broken_models: frozenset[str] = frozenset()
    """Python-first models whose arguments are invalid; their names still resolve."""

    @property
    def name(self) -> str:
        return self.bound.loaded.config.name

    def locate(self, file: str, pointer: str, *, key: bool = False) -> Location:
        doc = self.docs.get(file)
        return doc.locate(pointer, key=key) if doc else Location(file)

    def at(self, model: ResolvedModel, sub: str = "") -> Location:
        """A pointer below the model's `@model` decorator."""
        return model.site.locate(sub)

    def locate_model(self, model: ResolvedModel, field: str | None = None) -> Location:
        """Where the winning layer sets `field`, else the decorator's own `field`, else the
        decorator."""
        origin = model.origins.get(field) if field is not None else None
        if origin is not None:
            return origin.location
        own = field is not None and field in model.own_fields
        return model.site.locate(f"/{field}" if own else "")

    def locate_data_test(self, model: ResolvedModel, index: int, leaf: str = "") -> Location:
        """Where the `index`th data test of `model` (plus `leaf`, such as `/check`) is written."""
        if model.data_tests_at is None:
            return model.site.locate(f"/data_tests/{index}{leaf}")
        file, pointer = model.data_tests_at
        return self.locate(file, f"{pointer}/{index}{leaf}")


@dataclass(frozen=True)
class _Layer:
    name: Layer
    file: str
    settings: Mapping[str, Any]
    pointers: Mapping[str, str]
    """JSON pointer of each key in `file`."""


def _layer(
    name: Layer, file: str, base: str, settings: Mapping[str, Any], prefix: str = ""
) -> _Layer:
    return _Layer(name, file, settings, {k: f"{base}/{prefix}{k}" for k in settings})


def _merge(low: Mapping[str, Any], high: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(low)
    for key, value in high.items():
        below = out.get(key)
        if isinstance(value, dict) and isinstance(below, dict):
            value = _merge(below, value)
        out[key] = value
    return out


def _dump(settings: ModelSettings | None) -> dict[str, Any]:
    """The settings a layer sets, by YAML key (a `models:` tree's `+` prefix dropped)."""
    if settings is None:
        return {}
    dumped = settings.model_dump(
        include=set(_SETTING_FIELDS), exclude_unset=True, by_alias=True, mode="python"
    )
    return {k.removeprefix("+"): v for k, v in dumped.items()}


def _folders(file: str, roots: Sequence[str]) -> tuple[str, list[str]] | None:
    """The model_paths root a file lies under, and its folders below that root."""
    under = [r for r in roots if r == "" or file.startswith(r + "/")]
    if not under:
        return None
    root = max(under, key=len)
    return root, [p for p in posixpath.dirname(file)[len(root) :].split("/") if p]


def _inherited(
    bound: BoundProject,
    file: str,
    roots: Sequence[str],
    module: str,
) -> list[_Layer]:
    """The layers a model file inherits, lowest first: the root `+keys`, then the `models:`
    tree entry of each folder from the model_paths root down to the file's own. A module
    outside every model_paths root (found through `model_packages`) uses its dotted package
    path as folders."""
    tree = bound.loaded.config.models
    out = [_layer("project", PROJECT_FILE, "/models", _dump(tree), "+")]
    found = _folders(file, roots)
    parts = found[1] if found is not None else module.split(".")[:-1]
    pointer = "/models"
    for part in parts:
        sub = tree.folders.get(part)
        if sub is None:
            break
        tree, pointer = sub, f"{pointer}/{part}"
        out.append(_layer("folder", PROJECT_FILE, pointer, _dump(tree), "+"))
    return out


def merge_settings(layers: Sequence[Mapping[str, Any]]) -> ResolvedModelConfig:
    """Merge settings dicts (YAML keys), lowest precedence first, into a resolved config."""
    merged: dict[str, Any] = {}
    tags: list[str] = []
    for layer in layers:
        merged = _merge(merged, layer)
        tags.extend(t for t in layer.get("tags") or () if t not in tags)
    merged = {k: v for k, v in merged.items() if v is not None}
    merged["tags"] = tags
    return ResolvedModelConfig.model_validate(merged)


def _properties(
    bound: BoundProject,
) -> tuple[dict[str, tuple[str, str, PropertiesEntry]], list[tuple[str, str, PropertiesEntry]]]:
    """Properties entries by model name (the first one wins), and the later duplicates."""
    first: dict[str, tuple[str, str, PropertiesEntry]] = {}
    later: list[tuple[str, str, PropertiesEntry]] = []
    for lf in bound.loaded.model_files:
        for i, e in enumerate(lf.config.models):
            found = (lf.path, f"/models/{i}", e)
            if e.name in first:
                later.append(found)
            else:
                first[e.name] = found
    return first, later


def resolve_project(
    bound: BoundProject,
    python: Sequence[PythonModel] = (),
    broken: frozenset[str] = frozenset(),
) -> ResolvedProject:
    """Apply settings to every model and assign unique ids. It reports nothing: L2 does."""
    loaded = bound.loaded
    project = loaded.config.name
    # A root of "." is the project dir itself; file paths carry no "./", so it becomes "".
    roots = [p if p != "." else "" for p in map(posixpath.normpath, loaded.config.model_paths)]
    docs = {f.path: f.doc for f in loaded.model_files}
    docs[PROJECT_FILE] = loaded.project_file.doc

    def locate(file: str, pointer: str) -> Location:
        doc = docs.get(file)
        return doc.locate(pointer) if doc else Location(file)

    props, later = _properties(bound)

    def props_layer(name: str, layers: list[_Layer]) -> Origin | None:
        """Append the model's properties layer; the origin of its `columns`, if it sets them."""
        if name not in props:
            return None
        pfile, ppointer, pe = props[name]
        prop = _layer("properties", pfile, f"{ppointer}/config", _dump(pe.config))
        if pe.description is not None:
            prop = _Layer(
                prop.name,
                pfile,
                {**prop.settings, "description": pe.description},
                {**prop.pointers, "description": f"{ppointer}/description"},
            )
        layers.append(prop)
        if pe.columns is None:
            return None
        return Origin("properties", locate(pfile, f"{ppointer}/columns"))

    models: dict[str, ResolvedModel] = {}
    sources: dict[str, ResolvedSource] = {}
    shadowed_models: list[ResolvedModel] = []
    unit_tests: list[ResolvedUnitTest] = []
    for bs in bound.sources:
        name = f"{bs.group}.{bs.table}"
        sources[source_id(project, name)] = ResolvedSource(
            source_id(project, name), name, PROJECT_FILE, bs.pointer, bs.config
        )

    known = {p.name for p in python} | broken
    tables: dict[str, list[str]] = {}
    for s in sources.values():
        tables.setdefault(s.name.rsplit(".", 1)[-1], []).append(s.name)
    binding: list[Problem] = []
    for p in sorted(python, key=lambda p: (p.site.file, p.site.line, p.site.col)):
        layers = _inherited(bound, p.site.file, roots, p.module)
        columns = props_layer(p.name, layers)
        layers.append(_Layer("decorator", p.site.file, p.settings, {}))
        entry_columns = p.columns
        if p.columns is not None:
            columns = Origin("decorator", p.site.locate("/columns"))
        elif columns is not None:
            entry_columns = props[p.name][2].columns
        origins = {
            key: Origin(
                layer.name,
                p.site.locate(f"/{key}")
                if layer.name == "decorator"
                else locate(layer.file, layer.pointers[key]),
            )
            for layer in layers
            for key in layer.settings
        }
        if columns is not None:
            origins["columns"] = columns
        # data_tests is a model key: the decorator's list replaces the properties one.
        data_tests = p.data_tests
        props_at: tuple[str, str] | None = None
        if data_tests is not None:
            origins["data_tests"] = Origin("decorator", p.site.locate("/data_tests"))
        elif p.name in props and props[p.name][2].data_tests is not None:
            pfile, ppointer, pe = props[p.name]
            data_tests, props_at = pe.data_tests, (pfile, f"{ppointer}/data_tests")
            origins["data_tests"] = Origin("properties", locate(*props_at))
        inputs = _bind(p, project, known, tables, binding)
        m = ResolvedModel(
            unique_id=model_id(project, p.name),
            name=p.name,
            file=p.site.file,
            pointer=POINTER,
            folder=posixpath.dirname(p.site.file),
            entry=ModelConfig.model_construct(
                name=p.name,
                python=p.callable,
                inputs={
                    a: InputConfig.model_construct(ref=b.name)
                    if b.kind == "model"
                    else InputConfig.model_construct(source=b.name)
                    for a, b in inputs.items()
                },
                columns=entry_columns,
                # PLP405 judges only the write options the model declares itself.
                write=WriteConfig.model_validate(p.settings["write"])
                if "write" in p.settings
                else None,
                data_tests=data_tests,
            ),
            config=merge_settings([layer.settings for layer in layers]),
            inputs=inputs,
            own_fields=p.own_fields,
            origins=origins,
            data_tests_at=props_at,
            site=p.site,
        )
        if p.name in props and m.unique_id not in models:
            pfile, ppointer, pe = props[p.name]
            for j, case in enumerate(pe.unit_tests or ()):
                unit_tests.append(
                    ResolvedUnitTest(p.name, case.name, pfile, f"{ppointer}/unit_tests/{j}", case)
                )
        if m.unique_id in models:
            shadowed_models.append(m)
        else:
            models[m.unique_id] = m

    names = {m.name for m in models.values()} | broken
    problems = [
        PropertiesProblem(name, file, ptr, None)
        for name, (file, ptr, _) in props.items()
        if name not in names
    ]
    for file, ptr, e in later:
        first = props[e.name]
        there = locate(first[0], f"{first[1]}/name") if e.name in names else None
        problems.append(PropertiesProblem(e.name, file, ptr, there))
    return ResolvedProject(
        bound=bound,
        models=models,
        sources=sources,
        shadowed_models=tuple(shadowed_models),
        docs=docs,
        interpolations=loaded.interpolations,
        unit_tests=tuple(unit_tests),
        properties_problems=tuple(problems),
        binding_problems=tuple(binding),
        broken_models=broken,
    )


def _bind(
    p: PythonModel,
    project: str,
    models: set[str] | frozenset[str],
    tables: Mapping[str, Sequence[str]],
    problems: list[Problem],
) -> dict[str, InputBinding]:
    """Bind the parameters of a Python-first model to inputs (ADR-0050 point 3)."""
    out: dict[str, InputBinding] = {}
    ctx = {"model": p.name, "callable": p.callable}

    def problem(code: str, message: str, sub: str, hint: str | None = None, **more: str) -> None:
        problems.append(Problem(code, message, p.site.locate(sub), hint, {**ctx, **more}))

    def by_name(param: str, name: str, sub: str) -> None:
        if name != p.name and name in models:
            out[param] = InputBinding("model", model_id(project, name))
            return
        found = tables.get(name, ())
        if len(found) == 1:
            out[param] = InputBinding("source", source_id(project, found[0]))
        elif found:
            problem(
                "PLR210",
                f"input '{param}' of model '{p.name}' matches several source tables: "
                + ", ".join(sorted(found)),
                sub,
                hint=f"bind it with inputs={{'{param}': source(...)}}",
                input=param,
            )
        else:
            candidates = sorted({*models, *(n for v in tables.values() for n in v)} - {p.name})
            problem(
                "PLR201",
                f"input '{param}' of model '{p.name}' names no model or source table '{name}'",
                sub,
                hint=suggest(name, candidates),
                input=param,
            )

    params = {q.name: q for q in p.params}
    kwargs = any(q.kind == "VAR_KEYWORD" for q in p.params)
    for q in p.params:
        if q.name == "ctx" or q.kind == "VAR_KEYWORD":
            continue
        if q.kind in ("VAR_POSITIONAL", "POSITIONAL_ONLY"):
            kind = "*args" if q.kind == "VAR_POSITIONAL" else "a positional-only parameter"
            problem(
                "PLK602",
                f"parameters of '{p.callable}' cannot take {kind} ('{q.name}')",
                "",
                hint="inputs are passed by keyword",
                parameter=q.name,
            )
            continue
        given = p.inputs.get(q.name)
        if isinstance(given, InputConfig):
            if given.ref is not None:
                out[q.name] = InputBinding("model", model_id(project, given.ref))
            else:
                out[q.name] = InputBinding("source", source_id(project, given.source or ""))
        elif isinstance(given, str):
            by_name(q.name, given, f"/inputs/{q.name}")
        elif not q.has_default:
            by_name(q.name, q.name, "")
    for alias, given in p.inputs.items():
        if alias in params:
            continue
        if not kwargs:
            problem(
                "PLK602",
                f"input '{alias}' of model '{p.name}' is not a parameter of '{p.callable}'",
                f"/inputs/{alias}",
                hint="add the parameter, or take **kwargs",
                input=alias,
            )
        elif isinstance(given, InputConfig):
            out[alias] = (
                InputBinding("model", model_id(project, given.ref))
                if given.ref is not None
                else InputBinding("source", source_id(project, given.source or ""))
            )
        else:
            by_name(alias, given, f"/inputs/{alias}")
    return out
