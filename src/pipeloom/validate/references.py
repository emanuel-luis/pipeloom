"""L2: names resolve. Duplicate models and sources, unknown refs and unknown sources."""

from __future__ import annotations

from pipeloom.config.resolve import ResolvedProject
from pipeloom.diagnostics import Report, suggest


def check_references(
    project: ResolvedProject, report: Report, *, models_incomplete: bool = False
) -> None:
    """Report PLR201, PLR202, PLR203, PLR205 and PLR209. The caller stops before the graph on
    errors. `models_incomplete` (some model modules could not be loaded) suppresses PLR209,
    whose entries may describe the models that are missing."""
    model_names = [m.name for m in project.models.values()]
    for p in () if models_incomplete else project.properties_problems:
        if p.first is None:
            message = f"properties entry names no model '{p.name}'"
            hint = suggest(p.name, model_names)
        else:
            message = f"model '{p.name}' is already described by another properties entry"
            hint = f"first described in {p.first.file}:{p.first.line}"
        report.add(
            "PLR209",
            message,
            location=project.locate(p.file, f"{p.pointer}/name"),
            hint=hint,
            context={"model": p.name},
        )
    for kind, code, first_of, shadowed in (
        ("model", "PLR202", project.models, project.shadowed_models),
        ("source", "PLR205", project.sources, project.shadowed_sources),
    ):
        for dup in shadowed:
            first = first_of[dup.unique_id]
            there = project.locate(first.file, f"{first.pointer}/name")
            report.add(
                code,
                f"duplicate {kind} name '{dup.name}'",
                location=project.locate(dup.file, f"{dup.pointer}/name"),
                hint=f"first defined in {there.file}:{there.line}",
                context={"name": dup.name, "first": first.file},
            )
    source_names = [s.name for s in project.sources.values()]
    for m in project.models.values():
        for alias, binding in m.inputs.items():
            is_ref = binding.kind == "model"
            if binding.unique_id in (project.models if is_ref else project.sources):
                continue
            key = "ref" if is_ref else "source"
            report.add(
                "PLR201" if is_ref else "PLR203",
                f"unknown {key} '{binding.name}' in input '{alias}' of model '{m.name}'",
                location=project.locate(m.file, f"{m.pointer}/inputs/{alias}/{key}"),
                hint=suggest(binding.name, model_names if is_ref else source_names),
                context={"model": m.name, "input": alias, key: binding.name},
            )
