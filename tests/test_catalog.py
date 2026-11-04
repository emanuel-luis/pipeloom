from __future__ import annotations

import re
from pathlib import Path

import pytest

from pipeloom.diagnostics.codes import CATALOG, Layer, info, render_markdown

ROOT = Path(__file__).parent.parent
CODE = re.compile(r"\bPL[A-Z]{1,2}\d{3}\b")


def test_every_literal_in_src_is_in_the_catalog() -> None:
    used = {
        code
        for path in (ROOT / "src").rglob("*.py")
        for code in CODE.findall(path.read_text(encoding="utf-8"))
    }
    assert used - set(CATALOG) == set()


def test_every_active_code_has_a_case_file() -> None:
    cases = {p.stem for p in (ROOT / "tests" / "cases").glob("*.yml")}
    active = {c for c, i in CATALOG.items() if i.status == "active"}
    assert active - cases == set()
    assert cases - set(CATALOG) == set()


def test_codes_are_well_formed_and_sorted() -> None:
    assert list(CATALOG) == sorted(CATALOG)
    for code, entry in CATALOG.items():
        assert CODE.fullmatch(code) and entry.code == code
        assert isinstance(entry.layer, Layer)


def test_active_codes_and_others_reserved() -> None:
    active = {c for c, i in CATALOG.items() if i.status == "active"}
    assert active == (
        {f"PLY00{n}" for n in range(1, 5)}
        | {f"PLS10{n}" for n in range(1, 9)}
        | {"PLR201", "PLR202", "PLR203", "PLR204", "PLR205", "PLR206", "PLR207"}
        | {f"PLG30{n}" for n in range(1, 6)}
        | {"PLU001", "PLU002", "PLU003"}
    )
    assert info("PLP401").status == "reserved"


def test_unknown_code_raises() -> None:
    with pytest.raises(KeyError):
        info("PLZ999")


def test_render_markdown_has_a_section_per_code() -> None:
    text = render_markdown()
    for code in CATALOG:
        assert f"### {code} {{#{code.lower()}}}" in text
