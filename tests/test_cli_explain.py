"""`pipeloom explain`: every catalog code prints, input is case-insensitive, unknown codes fail."""

from __future__ import annotations

from typing import Any

import pytest
from click.testing import CliRunner

from pipeloom.cli import main
from pipeloom.diagnostics.codes import CATALOG, DOCS_BASE_URL, docs_url


def cli(*args: str) -> Any:
    return CliRunner().invoke(main, list(args), env={})


@pytest.mark.parametrize("code", list(CATALOG))
def test_every_catalog_code_prints(code: str) -> None:
    entry = CATALOG[code]
    r = cli("explain", code)
    assert r.exit_code == 0
    for part in (code, entry.title, entry.explanation, entry.since, docs_url(code)):
        assert part in r.stdout
    assert f"Status: {entry.status}" in r.stdout


def test_the_url_points_at_the_error_code_anchor() -> None:
    assert DOCS_BASE_URL == "https://emanuel-luis.github.io/pipeloom/latest/"
    assert docs_url("PLX101") == f"{DOCS_BASE_URL}reference/errors/#plx101"


def test_input_is_case_insensitive() -> None:
    assert cli("explain", "plx101").stdout == cli("explain", "PLX101").stdout


def test_a_reserved_code_says_so() -> None:
    r = cli("explain", "PLF701")
    assert r.exit_code == 0
    assert "Status: reserved" in r.stdout


def test_an_unknown_code_is_a_usage_error_with_a_suggestion() -> None:
    r = cli("explain", "PLX11")
    assert r.exit_code == 2
    assert "unknown code 'PLX11'" in r.stderr
    assert "did you mean" in r.stderr


def test_an_unknown_code_without_a_close_match() -> None:
    r = cli("explain", "nothing")
    assert r.exit_code == 2
    assert "did you mean" not in r.stderr
