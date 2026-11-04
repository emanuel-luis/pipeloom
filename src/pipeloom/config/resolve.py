"""Resolve a bound project: apply defaults to every model and give models and sources ids.

Settings precedence, highest first: the model entry, `_defaults.yml` files from the model's own
folder up to its model_paths root (nearest wins), `defaults.models` in pipeloom.yml, then the
built-in defaults of `ResolvedModelConfig`. Mappings merge deeply, lists replace, and `tags` is an
ordered union without duplicates. An explicit `null` removes a scalar or mapping set by a lower layer; `tags: null` is ignored.
"""

from __future__ import annotations

import posixpath
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from pipeloom.config.bind import BoundProject
from pipeloom.config.models import (
    DefaultsFile,
    ModelConfig,
    ModelSettings,
    ResolvedModelConfig,
    SourceConfig,
)
from pipeloom.config.yaml import LoadedDocument
from pipeloom.diagnostics import Location
from pipeloom.secrets.interpolation import InterpolationRecord

_SETTING_FIELDS = frozenset(ModelSettings.model_fields)


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
    file: str
    pointer: str
    """JSON pointer of the entry in its file, for example `/sources/0`."""
    config: SourceConfig


@dataclass(frozen=True)
class ResolvedModel:
    unique_id: str
    name: str
    file: str
    pointer: str
    """JSON pointer of the entry in its file, for example `/models/0`."""
    folder: str
    entry: ModelConfig
    config: ResolvedModelConfig
    inputs: Mapping[str, InputBinding]
    own_fields: frozenset[str]
    """YAML keys the entry sets itself."""


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

    @property
    def name(self) -> str:
        return self.bound.loaded.config.name

    def locate(self, file: str, pointer: str, *, key: bool = False) -> Location:
        doc = self.docs.get(file)
        return doc.locate(pointer, key=key) if doc else Location(file)

    def locate_model(self, model: ResolvedModel, field: str | None = None) -> Location:
        """The model's own `field` when it sets one, else its `name`."""
        leaf = field if field is not None and field in model.own_fields else "name"
        return self.locate(model.file, f"{model.pointer}/{leaf}")


def _merge(low: Mapping[str, Any], high: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(low)
    for key, value in high.items():
        below = out.get(key)
        if isinstance(value, dict) and isinstance(below, dict):
            value = _merge(below, value)
        out[key] = value
    return out


def _dump(settings: ModelSettings | ModelConfig) -> dict[str, Any]:
    return settings.model_dump(
        include=set(_SETTING_FIELDS), exclude_unset=True, by_alias=True, mode="python"
    )


def _chain(
    file: str, roots: Sequence[str], defaults: Mapping[str, Sequence[DefaultsFile]]
) -> list[ModelSettings]:
    """Defaults files that apply to a model file, root folder first, nearest last."""
    under = [r for r in roots if r == "" or file.startswith(r + "/")]
    if not under:
        return []
    root = max(under, key=len)
    folders = [root]
    for part in posixpath.dirname(file)[len(root) :].split("/"):
        if part:
            folders.append(f"{folders[-1]}/{part}" if folders[-1] else part)
    return [d.models for folder in folders for d in defaults.get(folder, ())]


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


def resolve_project(bound: BoundProject) -> ResolvedProject:
    """Apply defaults to every model and assign unique ids. It reports nothing: L2 does."""
    loaded = bound.loaded
    project = loaded.config.name
    # A root of "." is the project dir itself; file paths carry no "./", so it becomes "".
    roots = [p if p != "." else "" for p in map(posixpath.normpath, loaded.config.model_paths)]
    defaults: dict[str, list[DefaultsFile]] = {}
    for d in loaded.defaults_files:
        defaults.setdefault(posixpath.dirname(d.path), []).append(d.config)
    base = _dump(loaded.config.defaults.models)

    models: dict[str, ResolvedModel] = {}
    sources: dict[str, ResolvedSource] = {}
    shadowed_models: list[ResolvedModel] = []
    shadowed_sources: list[ResolvedSource] = []
    fields = ModelConfig.model_fields
    for lf in loaded.model_files:
        chain = [_dump(s) for s in _chain(lf.path, roots, defaults)]
        for i, entry in enumerate(lf.config.models):
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
                pointer=f"/models/{i}",
                folder=posixpath.dirname(lf.path),
                entry=entry,
                config=merge_settings([base, *chain, _dump(entry)]),
                inputs=inputs,
                own_fields=frozenset(fields[n].alias or n for n in entry.model_fields_set),
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
    docs = {f.path: f.doc for f in loaded.model_files}
    docs.update({f.path: f.doc for f in loaded.defaults_files})
    return ResolvedProject(
        bound=bound,
        models=models,
        sources=sources,
        shadowed_models=tuple(shadowed_models),
        shadowed_sources=tuple(shadowed_sources),
        docs=docs,
        interpolations=loaded.interpolations,
    )
