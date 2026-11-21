"""Every example project appears in the docs gallery, so a new example cannot be forgotten there."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "examples"
GALLERY = ROOT / "docs" / "examples" / "index.md"
# The sdist ships tests/ without examples/ or docs/.
if not EXAMPLES.is_dir() or not GALLERY.is_file():
    pytest.skip("examples/ or docs/ is not in this source tree", allow_module_level=True)
# The four TPC-H engine variants share one card, which links examples/README.md.
SHARED_CARD = {"tpch_polars", "tpch_pandas", "tpch_spark"}


def projects() -> list[str]:
    return sorted(
        p.name
        for p in EXAMPLES.iterdir()
        if p.is_dir() and p.name != "data" and not p.name.startswith(".")
    )


@pytest.mark.parametrize("name", projects())
def test_every_example_is_in_the_gallery(name: str) -> None:
    text = GALLERY.read_text(encoding="utf-8")
    if name in SHARED_CARD:
        assert "examples/README.md" in text
    else:
        assert name in text or name.replace("_", "-") in text, (
            f"examples/{name} has no card in {GALLERY.name}"
        )
