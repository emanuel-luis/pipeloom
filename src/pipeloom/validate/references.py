"""L2: names resolve. Duplicate models and sources, unknown refs and unknown sources."""

from __future__ import annotations

from functools import partial

from pipeloom.config.resolve import ResolvedModel, ResolvedProject, ResolvedUnitTest
from pipeloom.diagnostics import Report, suggest


def _plr208(
    project: ResolvedProject,
    report: Report,
    case: ResolvedUnitTest,
    message: str,
    leaf: str,
    hint: str | None = None,
) -> None:
    report.add(
        "PLR208",
        message,
        location=project.locate(case.file, f"{case.pointer}/{leaf}"),
        hint=hint,
        context={"model": case.model, "unit_test": case.name},
    )


def check_references(
    project: ResolvedProject, report: Report, *, models_incomplete: bool = False
) -> None:
    """Report PLR201, PLR202, PLR203, PLR205 and PLR209. The caller stops before the graph on
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
    for kind, code, first_of, shadowed in (
        ("model", "PLR202", project.models, project.shadowed_models),
        ("source", "PLR205", project.sources, project.shadowed_sources),
    ):
        for dup in shadowed:
            first = first_of[dup.unique_id]
            if isinstance(first, ResolvedModel) and isinstance(dup, ResolvedModel):
                there, here = project.at(first, "/name"), project.at(dup, "/name")
            else:
                there = project.locate(first.file, f"{first.pointer}/name")
                here = project.locate(dup.file, f"{dup.pointer}/name")
            report.add(
                code,
                f"duplicate {kind} name '{dup.name}'",
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
        for i, test in enumerate(m.entry.data_tests or ()):
            rel = test.check.relationships
            if rel is None:
                continue
            is_ref = rel.to.ref is not None
            name = rel.to.ref if is_ref else rel.to.source
            if name in (model_names if is_ref else source_names):
                continue
            if is_ref and name in project.broken_models:
                continue  # its own PLS code is reported
            key = "ref" if is_ref else "source"
            report.add(
                "PLR201" if is_ref else "PLR203",
                f"unknown {key} '{name}' in data test '{test.name}' of model '{m.name}'",
                location=project.locate_data_test(m, i, f"/check/relationships/to/{key}"),
                hint=suggest(name or "", model_names if is_ref else source_names),
                context={"model": m.name, "data_test": test.name, key: name},
            )
    _check_unit_tests(project, report, model_names)


def _check_unit_tests(project: ResolvedProject, report: Report, model_names: list[str]) -> None:
    """PLR208: a unit test names an unknown model, gives a wrong set of inputs, or reuses a
    case name of its model."""
    by_name = {m.name: m for m in project.models.values()}
    seen: set[tuple[str, str]] = set()
    for case in project.unit_tests:
        add = partial(_plr208, project, report, case)
        model = by_name.get(case.model)
        if model is None:
            add(
                f"unit test '{case.name}' names unknown model '{case.model}'",
                "model",
                suggest(case.model, model_names),
            )
            continue
        if (case.model, case.name) in seen:
            add(f"duplicate unit test name '{case.name}' for model '{case.model}'", "name")
        seen.add((case.model, case.name))
        for alias in case.config.given:
            if alias not in model.inputs:
                add(
                    f"unit test '{case.name}' gives '{alias}', which is not an input of model "
                    f"'{case.model}'",
                    f"given/{alias}",
                    suggest(alias, list(model.inputs)),
                )
        for alias in model.inputs:
            if alias not in case.config.given:
                add(
                    f"unit test '{case.name}' does not give input '{alias}' of model "
                    f"'{case.model}'",
                    "name",
                    "every input alias of the model needs given rows",
                )
