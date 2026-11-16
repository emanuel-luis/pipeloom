"""The authoring API: `@model`, its spec and the helpers (ADR-0050)."""

from __future__ import annotations

from typing import Any

import pytest

import pipeloom
from pipeloom import authoring
from pipeloom.authoring import NO_DEFAULT, ModelSpec, Ref, SourceRef, Var
from pipeloom.config import models as cfg
from pipeloom.runtime.context import ModelContext


def spec(fn: Any) -> ModelSpec:
    out = fn.__pipeloom_model__
    assert isinstance(out, ModelSpec)
    return out


def test_bare_form_returns_the_same_callable_function() -> None:
    def orders(x: int) -> int:
        return x + 1

    marked = pipeloom.model(orders)
    assert marked is orders and marked(1) == 2
    assert spec(orders).kwargs == {}


def test_called_form_keeps_only_given_arguments() -> None:
    @pipeloom.model(materialized="view", tags=["a"], description=None)
    def orders() -> str:
        return "rows"

    assert orders() == "rows"
    assert dict(spec(orders).kwargs) == {"materialized": "view", "tags": ["a"]}
    assert spec(pipeloom.model()(lambda: 1)).kwargs == {}


def test_the_spec_is_frozen() -> None:
    s = spec(pipeloom.model(schema="s")(lambda: 1))
    with pytest.raises(AttributeError):
        s.kwargs = {}  # type: ignore[misc]
    with pytest.raises(TypeError):
        s.kwargs["schema"] = "t"  # type: ignore[index]


def test_arguments_are_not_validated_when_decorating() -> None:
    fn = pipeloom.model(materialized="nope", retries=-1)(lambda: 1)  # type: ignore[call-overload]
    assert spec(fn).kwargs == {"materialized": "nope", "retries": -1}


def test_decorating_twice_raises() -> None:
    def orders() -> None: ...

    pipeloom.model(orders)
    with pytest.raises(TypeError, match="applied twice"):
        pipeloom.model(name="x")(orders)


def test_input_and_var_helpers() -> None:
    assert pipeloom.ref("a") == Ref("a")
    assert pipeloom.source("tpch", "orders") == SourceRef("tpch", "orders")
    assert pipeloom.var("v") == Var("v", NO_DEFAULT)
    assert pipeloom.var("v", default=None).default is None


@pytest.mark.parametrize(
    ("check", "expected"),
    [
        (pipeloom.not_null("a", "b"), {"not_null": ["a", "b"]}),
        (pipeloom.unique("a"), {"unique": ["a"]}),
        (
            pipeloom.accepted_values("s", ("x", "y")),
            {"accepted_values": {"column": "s", "values": ["x", "y"]}},
        ),
        (pipeloom.between("p", min=0), {"range": {"column": "p", "min": 0, "max": None}}),
        (pipeloom.regex("e", ".+@.+"), {"regex": {"column": "e", "pattern": ".+@.+"}}),
        (pipeloom.row_count(min=1), {"row_count": {"min": 1, "max": None}}),
        (pipeloom.freshness("t", "1h"), {"freshness": {"column": "t", "max_age": 3600}}),
    ],
)
def test_check_helpers_build_check_configs(check: cfg.CheckConfig, expected: Any) -> None:
    assert isinstance(check, cfg.CheckConfig)
    assert check == cfg.CheckConfig.model_validate(expected)


def test_aliases_are_the_config_classes() -> None:
    assert pipeloom.Write is cfg.WriteConfig
    assert pipeloom.Incremental is cfg.IncrementalConfig
    assert pipeloom.Column is cfg.ColumnConfig
    assert pipeloom.Expectation is cfg.ExpectationConfig
    assert pipeloom.Quality is cfg.QualityConfig
    assert pipeloom.Deletes is cfg.DeletesConfig
    assert pipeloom.ModelContext is ModelContext


def test_the_root_exports_the_authoring_api() -> None:
    internal = {"MARKER", "NO_DEFAULT", "ModelSpec", "Ref", "SourceRef", "Var"}
    assert set(authoring.__all__) - internal <= set(pipeloom.__all__)
