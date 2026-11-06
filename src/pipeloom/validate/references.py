"""L2: names resolve. Duplicate models and sources, unknown refs and unknown sources."""

from __future__ import annotations

from functools import partial

from pipeloom.config.resolve import ResolvedProject, ResolvedUnitTest
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


def check_references(project: ResolvedProject, report: Report) -> None:
    """Report PLR201, PLR202, PLR203 and PLR205. The caller stops before the graph on errors."""
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
    model_names = [m.name for m in project.models.values()]
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
