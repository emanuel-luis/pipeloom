"""MkDocs hook: generate the error-code page, the engine capability matrix and the JSON Schemas."""

from __future__ import annotations

import dataclasses
from typing import Any

from pipeloom.artifacts.schema import artifact_schemas
from pipeloom.config.schema import dump_schema, export_schemas
from pipeloom.diagnostics import Report
from pipeloom.diagnostics.codes import render_markdown
from pipeloom.engine.types import EngineCapabilities
from pipeloom.plugins import PluginRegistry


def render_capability_matrix(engines: list[Any]) -> str:
    """A table of every `EngineCapabilities` field, one column per engine plugin spec."""
    engines = sorted(engines, key=lambda s: s.name)
    caps = {s.name: s.capabilities for s in engines}
    head = "| Capability | " + " | ".join(f"`{n}`" for n in caps) + " |"
    rows = [head, "| --- |" + " --- |" * len(caps)]
    for f in dataclasses.fields(EngineCapabilities):
        values = [getattr(c, f.name) for c in caps.values()]
        if isinstance(values[0], frozenset):
            for item in sorted(set().union(*values)):
                cells = ("yes" if item in v else "no" for v in values)
                rows.append(f"| {f.name} `{item}` | " + " | ".join(cells) + " |")
        elif isinstance(values[0], bool):
            rows.append(f"| {f.name} | " + " | ".join("yes" if v else "no" for v in values) + " |")
        else:
            rows.append(
                f"| {f.name} | " + " | ".join(f"`{v}`" if v else "none" for v in values) + " |"
            )
    return "\n".join(
        [
            "# Engine capabilities",
            "",
            "Generated from the `EngineCapabilities` each engine plugin declares. The engine "
            "conformance kit skips a cell the engine does not declare and fails one it does; see "
            "[Writing an engine](../plugins/writing-an-engine.md).",
            "",
            *rows,
            "",
        ]
    )


def _installed_engines() -> list[Any]:
    """Engine specs of the built-ins and of every installed distribution (entry points)."""
    report = Report()
    registry = PluginRegistry.discover(autoload=True, env={}, report=report)
    loaded: Any = registry.load_all(report, "engines")
    if report.has_errors:
        raise RuntimeError(
            "cannot load the engine plugins: " + "; ".join(d.message for d in report.sorted())
        )
    return list(loaded["engines"])


def generated_pages() -> dict[str, str]:
    """Docs path to content: the error catalog, the config schemas with the built-in plugins,
    and the artifact schemas."""
    report = Report()
    registry = PluginRegistry.discover(autoload=False, env={}, report=report)
    specs: Any = registry.load_all(report)
    if report.has_errors:
        raise RuntimeError(
            "cannot load the built-in plugins: " + "; ".join(d.message for d in report.sorted())
        )
    pages = {
        "reference/errors.md": render_markdown(),
        "reference/engine-capabilities.md": render_capability_matrix(_installed_engines()),
    }
    for name, doc in sorted({**export_schemas(specs), **artifact_schemas()}.items()):
        pages[f"schema/v1/{name}"] = dump_schema(doc)
    return pages


def on_files(files: Any, config: Any) -> Any:
    from mkdocs.structure.files import File

    for path, content in generated_pages().items():
        files.append(File.generated(config, path, content=content))
    return files
