"""Which source locations the runner resolves against the project directory."""

from __future__ import annotations

import pytest

from pipeloom.platform import is_absolute_location


@pytest.mark.parametrize(
    "location",
    [
        "/data/nation.csv",
        "/Volumes/cat/schema/vol/nation.csv",
        "dbfs:/Volumes/cat/schema/vol/nation.csv",
        "file:/tmp/nation.csv",
        "file:///tmp/nation.csv",
        "s3://bucket/nation.csv",
        "C:/data/nation.csv",
    ],
)
def test_kept_as_written(location: str) -> None:
    assert is_absolute_location(location)


@pytest.mark.parametrize("location", ["data/nation.csv", "./data/nation.csv", "nation.csv"])
def test_resolved_against_the_project(location: str) -> None:
    assert not is_absolute_location(location)
