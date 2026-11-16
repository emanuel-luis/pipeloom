"""L2: names resolve. Duplicate models, unknown refs and unknown sources."""

from __future__ import annotations

from pipeloom.config.resolve import ResolvedProject
from pipeloom.diagnostics import Report, suggest


def check_references(
    project: ResolvedProject, report: Report, *, models_incomplete: bool = False
) -> None:
    """Report PLR201, PLR202, PLR203 and PLR209. The caller stops before the graph on
    errors. `models_incomplete` (some model modules could not be loaded) suppresses PLR209,
    whose entries may describe the models that are missing."""
    model_names = [m.name for m in project.models.values()]
    for b in project.binding_problems:
        if b.code == "PLK602":
            continue  # L6: check_static reports it
        report.add(b.code, b.message, location=b.location, hint=b.hint, context=dict(b.context))
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
    for dup in project.shadowed_models:
        first = project.models[dup.unique_id]
        there, here = project.at(first, "/name"), project.at(dup, "/name")
        report.add(
            "PLR202",
            f"duplicate model name '{dup.name}'",
            location=here,
            hint=f"first defined in {there.file}:{there.line}",
            context={"name": dup.name, "first": first.file},
        )
    source_names = [s.name for s in project.sources.values()]
    for m in project.models.values():
        for alias, binding in m.inputs.items():
            is_ref = binding.kind == "model"
            if binding.unique_id in (project.models if is_ref else project.sources):
                continue
            if is_ref and binding.name in project.broken_models:
                continue  # its own PLS code is reported
            key = "ref" if is_ref else "source"
            report.add(
                "PLR201" if is_ref else "PLR203",
                f"unknown {key} '{binding.name}' in input '{alias}' of model '{m.name}'",
                location=project.at(m, f"/inputs/{alias}/{key}"),
                hint=suggest(binding.name, model_names if is_ref else source_names),
                context={"model": m.name, "input": alias, key: binding.name},
            )
