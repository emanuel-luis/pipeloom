"""The AST resolver: alias forms, static and dynamic classification, helper evaluation."""

from __future__ import annotations

import textwrap
from typing import Any

import pytest

from pipeloom.authoring import NO_DEFAULT, Ref, SourceRef, Var
from pipeloom.config import models as cfg
from pipeloom.discovery.ast_resolver import ParsedFile, build_models, parse_file
from pipeloom.discovery.model import DiscoveredModel, Param


def parse(src: str) -> ParsedFile:
    return parse_file(textwrap.dedent(src).encode(), "m.py")


def models(src: str) -> list[DiscoveredModel]:
    parsed = parse(src)
    assert parsed.kind == "static", parsed
    found, failures = build_models(parsed, "m.py", "m")
    assert failures == []
    return found


@pytest.mark.parametrize(
    "header, deco",
    [
        ("from pipeloom import model", "model"),
        ("from pipeloom import model as m", "m"),
        ("import pipeloom", "pipeloom.model"),
        ("import pipeloom as pl", "pl.model"),
        ("from pipeloom.authoring import model", "model"),
        ("import pipeloom.authoring", "pipeloom.authoring.model"),
        ("import pipeloom.authoring as pa", "pa.model"),
        ("from pipeloom import authoring", "authoring.model"),
    ],
)
def test_alias_forms_are_static(header: str, deco: str) -> None:
    src = f"{header}\n\n@{deco}(tags=['a'])\ndef orders(x, y=1):\n    return x\n"
    (m,) = models(src)
    assert (m.name, m.attr, m.line, m.col) == ("orders", "orders", 3, 2)
    assert m.kwargs == {"tags": ["a"]}
    assert m.kwarg_locations == {"tags": (3, len(deco) + 3)}
    assert m.params == (
        Param("x", "POSITIONAL_OR_KEYWORD"),
        Param("y", "POSITIONAL_OR_KEYWORD", True),
    )


@pytest.mark.parametrize(
    "header, call",
    [
        ("from pipeloom import Write, not_null as nn", "Write(strategy='merge', unique_key=['k'])"),
        ("import pipeloom as pl", "pl.Write(strategy='merge', unique_key=['k'])"),
        ("from pipeloom.authoring import Write", "Write(strategy='merge', unique_key=['k'])"),
    ],
)
def test_helper_and_class_calls_are_evaluated(header: str, call: str) -> None:
    src = f"""
    from pipeloom import model, ref, source, var, not_null
    {header}

    @model(
        write={call},
        inputs={{"a": ref("a"), "b": source("g", "t")}},
        retries=var("r", default=2),
        meta={{"k": (1, -2.5, b"x", 1j), 3: {{1, 2}}, "v": var("v")}},
        expectations=[{{"name": "e", "check": not_null("c")}}],
        description=None,
    )
    def orders(*, a, b, **rest):
        ...
    """
    (m,) = models(src)
    assert m.kwargs["write"] == cfg.WriteConfig(strategy="merge", unique_key=["k"])
    assert m.kwargs["inputs"] == {"a": Ref("a"), "b": SourceRef("g", "t")}
    assert m.kwargs["retries"] == Var("r", 2)
    assert m.kwargs["meta"] == {"k": (1, -2.5, b"x", 1j), 3: {1, 2}, "v": Var("v", NO_DEFAULT)}
    assert m.kwargs["expectations"] == [{"name": "e", "check": cfg.CheckConfig(not_null=["c"])}]
    assert "description" not in m.kwargs and "description" not in m.kwarg_locations
    assert [p.kind for p in m.params] == ["KEYWORD_ONLY", "KEYWORD_ONLY", "VAR_KEYWORD"]


def test_bare_decorator_and_several_models_per_file() -> None:
    src = """
    from pipeloom import model

    @model
    def a(): ...

    @model(name="bee")
    def b(a, /, c): ...
    """
    a, b = models(src)
    assert (a.name, a.kwargs, a.kwarg_locations, a.line) == ("a", {}, {}, 4)
    assert (b.name, b.attr) == ("bee", "b")
    assert [p.kind for p in b.params] == ["POSITIONAL_ONLY", "POSITIONAL_OR_KEYWORD"]


@pytest.mark.parametrize(
    "src",
    [
        # a non-static argument
        "from pipeloom import model\nX = 1\n@model(retries=X)\ndef a(): ...\n",
        "from pipeloom import model\n@model(tags=['a' + 'b'])\ndef a(): ...\n",
        "from pipeloom import model\n@model(**{'tags': []})\ndef a(): ...\n",
        "from pipeloom import model\nfrom x import helper\n@model(write=helper())\ndef a(): ...\n",
        # model used other than as a static decorator on a top-level def
        "from pipeloom import model\ndef a(): ...\nb = model(name='b')(a)\n",
        "from pipeloom import model\ndef make():\n    @model\n    def a(): ...\n    return a\n",
        "from pipeloom import model\nfor n in 'ab':\n    globals()[n] = model(lambda: 1)\n",
        "import functools\nfrom pipeloom import model\n@functools.cache\n@model\ndef a(): ...\n",
        # star import
        "from pipeloom import *\n@model\ndef a(): ...\n",
        # a rebound helper name
        "from pipeloom import model, ref\nref = str\n@model(inputs={'a': ref('a')})\ndef b(a): ...\n",
        "from pipeloom import model\ndef model(f): return f\n",
        "import pipeloom as pl\ntry:\n    pass\nexcept Exception as pl:\n    pass\n",
        # pipeloom imported inside a function
        "def make():\n    from pipeloom import model\n    return model\n",
    ],
)
def test_dynamic_rules(src: str) -> None:
    parsed = parse(src)
    assert parsed.kind == "dynamic"
    assert parsed.models == [] and parsed.failures == []


def test_dynamic_file_keeps_ast_sites() -> None:
    parsed = parse("from pipeloom import model\nX = 1\n\n@model(retries=X, tags=[])\ndef a(): ...\n")
    assert parsed.kind == "dynamic"
    site = parsed.sites["a"]
    assert (site.line, site.col, site.kwarg_locations) == (4, 2, {"retries": (4, 8), "tags": (4, 19)})


@pytest.mark.parametrize(
    "src",
    [
        "x = 1\n",
        "import os\n@model\ndef a(): ...\n",  # never imports pipeloom
        "from pipeloom import ModelContext\ndef helper(ctx: ModelContext): ...\n",
        "# pipeloom\nfrom pipeloom import model\n",
    ],
)
def test_files_without_models_are_none(src: str) -> None:
    assert parse(src).kind == "none"


def test_function_parameters_may_shadow_helpers() -> None:
    src = "from pipeloom import model, source\n@model\ndef a(source, ref=1):\n    return source\n"
    assert parse(src).kind == "static"


def test_unknown_keyword_is_a_failure_with_a_suggestion() -> None:
    src = "from pipeloom import model\n\n@model(retires=1)\ndef a(): ...\n\n@model\ndef b(): ...\n"
    parsed = parse(src)
    (failure,) = parsed.failures
    assert (failure.kind, failure.line, failure.col) == ("unknown_keyword", 3, 8)
    assert "'retires'" in failure.message and "did you mean 'retries'?" in failure.message
    found, _ = build_models(parsed, "m.py", "m")
    assert [m.name for m in found] == ["b"]


def test_unknown_keyword_is_checked_in_dynamic_modules() -> None:
    parsed = parse("from pipeloom import model\ndef make(f):\n    return model(materialised='view')(f)\n")
    assert parsed.kind == "dynamic"
    assert [f.kind for f in parsed.failures] == ["unknown_keyword"]


def test_invalid_helper_arguments_are_located_at_the_call() -> None:
    src = """
    from pipeloom import Write, model

    @model(
        write=Write(strategy="nope"),
    )
    def a(): ...
    """
    parsed = parse(src)
    assert parsed.kind == "static"
    found, failures = build_models(parsed, "m.py", "m")
    assert found == []
    (f,) = failures
    assert (f.kind, f.line, f.col) == ("invalid_argument", 5, 11)
    assert f.message.startswith("invalid Write(...)")


def test_var_in_a_typed_constructor_is_an_invalid_argument() -> None:
    src = "from pipeloom import Write, model, var\n@model(write=Write(unique_key=var('k')))\ndef a(): ...\n"
    _, failures = build_models(parse(src), "m.py", "m")
    assert [f.kind for f in failures] == ["invalid_argument"]


def test_syntax_error_is_located() -> None:
    parsed = parse("import pipeloom\ndef a(:\n")
    (f,) = parsed.failures
    assert (f.kind, f.line) == ("syntax", 2)


def test_kwargs_match_what_the_decorator_records() -> None:
    import pipeloom

    src = "from pipeloom import model, var\n@model(tags=('a',), retries=None, meta={'v': var('v')})\ndef a(): ...\n"
    (m,) = models(src)

    @pipeloom.model(tags=("a",), retries=None, meta={"v": pipeloom.var("v")})
    def a() -> None: ...

    spec: Any = a.__pipeloom_model__  # type: ignore[attr-defined]
    assert dict(spec.kwargs) == m.kwargs
