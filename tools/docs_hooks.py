"""MkDocs hook: generate the error-code page and the JSON Schemas from the code."""

from __future__ import annotations

from typing import Any

from pipeloom.config.schema import dump_schema, export_schemas
from pipeloom.diagnostics import Report
from pipeloom.diagnostics.codes import render_markdown
from pipeloom.plugins import PluginRegistry


def generated_pages() -> dict[str, str]:
    """Docs path to content: the error catalog and the schemas of the built-in plugins."""
    report = Report()
    registry = PluginRegistry.discover(autoload=False, env={}, report=report)
    specs: Any = registry.load_all(report)
    if report.has_errors:
        raise RuntimeError(
            "cannot load the built-in plugins: " + "; ".join(d.message for d in report.sorted())
        )
    pages = {"reference/errors.md": render_markdown()}
    for name, doc in sorted(export_schemas(specs).items()):
        pages[f"schema/v1/{name}"] = dump_schema(doc)
    return pages


def on_files(files: Any, config: Any) -> Any:
    from mkdocs.structure.files import File

    for path, content in generated_pages().items():
        files.append(File.generated(config, path, content=content))
    return files
