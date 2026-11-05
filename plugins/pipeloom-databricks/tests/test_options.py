from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipeloom_databricks.options import (
    DatabricksDeployOptions,
    JobClusterCompute,
    ServerlessCompute,
)
from pipeloom_databricks.spec import RENDERER_SPEC


def test_defaults() -> None:
    opts = DatabricksDeployOptions()
    assert list(opts.pipelines) == ["main"]
    assert opts.compute == {"default": ServerlessCompute()}
    assert opts.default_compute == "default"
    assert opts.task_grouping == "none"
    assert opts.settings == {} and opts.compute_rules == []


def test_compute_is_chosen_by_kind() -> None:
    opts = DatabricksDeployOptions.model_validate(
        {
            "compute": {
                "a": {"kind": "serverless", "environment_version": "6"},
                "b": {"kind": "job_cluster", "new_cluster": {"num_workers": 2}},
            }
        }
    )
    assert isinstance(opts.compute["a"], ServerlessCompute)
    assert isinstance(opts.compute["b"], JobClusterCompute)
    assert opts.compute["b"].libraries is None


@pytest.mark.parametrize(
    "bad",
    [
        {"unknown": 1},
        {"task_grouping": "by_owner"},
        {"compute": {"x": {"kind": "job_cluster"}}},
        {"compute": {"x": {"kind": "gpu"}}},
        {"compute": {"not an identifier": {"kind": "serverless"}}},
        {"compute_rules": [{"select": ["tag:a"]}]},
    ],
)
def test_invalid_options_are_rejected(bad: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        DatabricksDeployOptions.model_validate(bad)


def test_the_spec_binds_these_options() -> None:
    assert RENDERER_SPEC.config_model is DatabricksDeployOptions
    opts = DatabricksDeployOptions()
    assert RENDERER_SPEC.factory(opts) is opts
