"""Resolve a bound project: apply settings to every model and give models and sources ids.

Settings precedence, highest first:

1. the model's own entry (YAML today; the decorator layer of ADR-0050 replaces it);
2. its properties entry: `config`, plus `description` and `columns`;
3. the `models:` tree in pipeloom.yml, from the model's folder up to its model_paths root;
4. the root `+key` settings of that tree;
5. legacy `_defaults.yml` files, nearest folder first, then legacy `defaults.models`. They stay
   below the new layers until the change that removes them;
6. the built-in defaults of `ResolvedModelConfig`.

Mappings merge deeply, lists replace, and `tags` is an ordered union without duplicates. An
explicit `null` removes a scalar or mapping set by a lower layer; `tags: null` is ignored. A model
entry's `columns` replace the properties `columns` whole.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, NamedTuple

from pipeloom.config.bind import BoundProject
from pipeloom.config.discover import PROJECT_FILE
from pipeloom.config.models import (
    DefaultsFile,
    ModelConfig,
    ModelSettings,
    PropertiesEntry,
    ResolvedModelConfig,
    SourceConfig,
)
from pipeloom.config.yaml import LoadedDocument
from pipeloom.diagnostics import Location
from pipeloom.secrets.interpolation import InterpolationRecord

_SETTING_FIELDS = frozenset(ModelSettings.model_fields)

Layer = Literal["model", "properties", "folder", "project", "defaults_file", "project_defaults"]


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
    """JSON pointer of the entry in its file, for example `/sources/0`."""
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
    """JSON pointer of the entry in its file, for example `/models/0`."""
    folder: str
    entry: ModelConfig
    """The model entry; its `columns` come from the properties entry when it sets none."""
    config: ResolvedModelConfig
    inputs: Mapping[str, InputBinding]
    own_fields: frozenset[str]
    """YAML keys the entry sets itself."""
    origins: Mapping[str, Origin] = field(default_factory=dict)
    """The winning layer of each setting, `description` and `columns` that some layer sets."""


@dataclass(frozen=True)
class PropertiesProblem:
    """A properties entry that names no model, or a model another entry already describes."""

    name: str
    file: str
    pointer: str
    first: Location | None
    """The entry that already describes the model, when there is one."""


@dataclass(frozen=True)
class ResolvedProject:
    bound: BoundProject
    models: Mapping[str, ResolvedModel]
    """Keyed by unique id, in file order. The first definition of a name wins."""
    sources: Mapping[str, ResolvedSource]
    shadowed_models: tuple[ResolvedModel, ...]
    """Later definitions of a model name already taken (PLR202)."""
    shadowed_sources: tuple[ResolvedSource, ...]
    docs: Mapping[str, LoadedDocument]
    interpolations: tuple[InterpolationRecord, ...]
    properties_problems: tuple[PropertiesProblem, ...] = ()
    """Properties entries that describe no model or a model twice (PLR209)."""

    @property
    def name(self) -> str:
        return self.bound.loaded.config.name

    def locate(self, file: str, pointer: str, *, key: bool = False) -> Location:
        doc = self.docs.get(file)
        return doc.locate(pointer, key=key) if doc else Location(file)

    def locate_model(self, model: ResolvedModel, field: str | None = None) -> Location:
        """Where the winning layer sets `field`, else the entry's own `field`, else its name."""
        origin = model.origins.get(field) if field is not None else None
        if origin is not None:
            return origin.location
        leaf = field if field is not None and field in model.own_fields else "name"
        return self.locate(model.file, f"{model.pointer}/{leaf}")


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


def _defaults_chain(
    file: str, roots: Sequence[str], defaults: Mapping[str, Sequence[tuple[str, DefaultsFile]]]
) -> list[_Layer]:
    """Defaults files that apply to a model file, root folder first, nearest last."""
    found = _folders(file, roots)
    if found is None:
        return []
    root, parts = found
    folders = [root]
    for part in parts:
        folders.append(f"{folders[-1]}/{part}" if folders[-1] else part)
    return [
        _layer("defaults_file", path, "/models", _dump(d.models))
        for folder in folders
        for path, d in defaults.get(folder, ())
    ]


def _tree_chain(bound: BoundProject, file: str, roots: Sequence[str]) -> list[_Layer]:
    """The `models:` tree: root `+keys` first, then each folder down to the file's own."""
    tree = bound.loaded.config.models
    out = [_layer("project", PROJECT_FILE, "/models", _dump(tree), "+")]
    found = _folders(file, roots)
    pointer = "/models"
    for part in found[1] if found else ():
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
            if isinstance(e, PropertiesEntry):
                found = (lf.path, f"/models/{i}", e)
                if e.name in first:
                    later.append(found)
                else:
                    first[e.name] = found
    return first, later


def resolve_project(bound: BoundProject) -> ResolvedProject:
    """Apply settings to every model and assign unique ids. It reports nothing: L2 does."""
    loaded = bound.loaded
    project = loaded.config.name
    # A root of "." is the project dir itself; file paths carry no "./", so it becomes "".
    roots = [p if p != "." else "" for p in map(posixpath.normpath, loaded.config.model_paths)]
    defaults: dict[str, list[tuple[str, DefaultsFile]]] = {}
    for d in loaded.defaults_files:
        defaults.setdefault(posixpath.dirname(d.path), []).append((d.path, d.config))
    base = _layer(
        "project_defaults", PROJECT_FILE, "/defaults/models", _dump(loaded.config.defaults.models)
    )
    docs = {f.path: f.doc for f in loaded.model_files}
    docs.update({f.path: f.doc for f in loaded.defaults_files})
    docs[PROJECT_FILE] = loaded.project_file.doc

    def locate(file: str, pointer: str) -> Location:
        doc = docs.get(file)
        return doc.locate(pointer) if doc else Location(file)

    props, later = _properties(bound)
    models: dict[str, ResolvedModel] = {}
    sources: dict[str, ResolvedSource] = {}
    shadowed_models: list[ResolvedModel] = []
    shadowed_sources: list[ResolvedSource] = []
    for bs in bound.sources:
        name = f"{bs.group}.{bs.table}"
        sources[source_id(project, name)] = ResolvedSource(
            source_id(project, name), name, PROJECT_FILE, bs.pointer, bs.config
        )
    fields = ModelConfig.model_fields
    for lf in loaded.model_files:
        inherited = [
            base,
            *_defaults_chain(lf.path, roots, defaults),
            *_tree_chain(bound, lf.path, roots),
        ]
        for i, entry in enumerate(lf.config.models):
            if not isinstance(entry, ModelConfig):
                continue
            pointer = f"/models/{i}"
            layers = list(inherited)
            columns: Origin | None = None
            if entry.name in props:
                pfile, ppointer, pe = props[entry.name]
                prop = _layer("properties", pfile, f"{ppointer}/config", _dump(pe.config))
                if pe.description is not None:
                    prop = _Layer(
                        prop.name,
                        pfile,
                        {**prop.settings, "description": pe.description},
                        {**prop.pointers, "description": f"{ppointer}/description"},
                    )
                layers.append(prop)
                if pe.columns is not None:
                    columns = Origin("properties", locate(pfile, f"{ppointer}/columns"))
            own = _dump(entry)
            if entry.description is not None:
                own["description"] = entry.description
            layers.append(_layer("model", lf.path, pointer, own))
            if entry.columns is not None:
                columns = Origin("model", locate(lf.path, f"{pointer}/columns"))
            elif columns is not None:
                entry = entry.model_copy(update={"columns": props[entry.name][2].columns})
            origins = {
                key: Origin(layer.name, locate(layer.file, layer.pointers[key]))
                for layer in layers
                for key in layer.settings
            }
            if columns is not None:
                origins["columns"] = columns
            inputs: dict[str, InputBinding] = {}
            for alias, ic in entry.inputs.items():
                if ic.ref is not None:
                    inputs[alias] = InputBinding("model", model_id(project, ic.ref))
                else:
                    inputs[alias] = InputBinding("source", source_id(project, ic.source or ""))
            m = ResolvedModel(
                unique_id=model_id(project, entry.name),
                name=entry.name,
                file=lf.path,
                pointer=pointer,
                folder=posixpath.dirname(lf.path),
                entry=entry,
                config=merge_settings([layer.settings for layer in layers]),
                inputs=inputs,
                own_fields=frozenset(
                    fields[n].alias or n for n in lf.config.models[i].model_fields_set
                ),
                origins=origins,
            )
            if m.unique_id in models:
                shadowed_models.append(m)
            else:
                models[m.unique_id] = m
        for i, src in enumerate(lf.config.sources):
            s = ResolvedSource(
                source_id(project, src.name), src.name, lf.path, f"/sources/{i}", src
            )
            if s.unique_id in sources:
                shadowed_sources.append(s)
            else:
                sources[s.unique_id] = s
    names = {m.name for m in models.values()}
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
        shadowed_sources=tuple(shadowed_sources),
        docs=docs,
        interpolations=loaded.interpolations,
        properties_problems=tuple(problems),
    )
