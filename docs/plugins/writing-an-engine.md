# Writing an engine

An engine is a distribution that registers a `PluginSpec` in the `pipeloom.engines` entry-point
group. This page walks through the pieces and ends with the conformance kit, which proves the
engine does what the core expects. The protocols are `Engine` and `EngineSession` in
`pipeloom.engine.protocols`; the design is in [ADR-0002](../adr/0002-engine-abstraction.md).

## 1. Declare what the engine can do

`EngineCapabilities` is static data that the core reads without importing the engine. Declare
only what the engine really does: the kit skips a cell for a capability you leave out and fails
one you claim and break. [Engine capabilities](../reference/engine-capabilities.md) lists the
declarations of the engines in this repository.

```python
# my_engine/spec.py
from pipeloom.engine.types import EngineCapabilities
from pipeloom.plugins import API_VERSION, OptionsModel, PluginSpec

CAPABILITIES = EngineCapabilities(
    formats=frozenset({"parquet", "csv"}),
    strategies=frozenset({"append", "overwrite"}),
    materializations=frozenset({"table", "view"}),
    sql_models=True,
    views=True,
)


class MyOptions(OptionsModel):
    """The engine has no options."""


def _build(opts):
    from my_engine.engine import MyEngine  # import the library on first use only

    return MyEngine()


SPEC = PluginSpec(
    "my-engine", "my-engine", "engines", API_VERSION, _build, MyOptions, capabilities=CAPABILITIES
)
```

Register it in `pyproject.toml`:

```toml
[project.entry-points."pipeloom.engines"]
my-engine = "my_engine.spec:SPEC"
```

## 2. Implement the engine

`Engine.session(ctx, catalog=...)` returns an `EngineSession`: a reader per format, a
materializer per write strategy, and `sql`, `schema`, `count`, `max_value`, `align` and the rest
of the protocol. `pipeloom-polars` is the shortest real example. A write takes an idempotency
token: repeating a token must not write twice.

## 3. Write a harness

The kit cannot build your engine's frames or files, so you give it a function that takes a
`tmp_path` and returns an `EngineHarness`: the engine, a catalog, and small adapters
(`make_native`, `to_rows`, `write_source`, `failing_relation`, `settings_snapshot`, plus
`expr_cases` and `update_condition` for the capabilities that need them). The field docstrings
of `pipeloom.testing.contract.engine.EngineHarness` say what each one must do.
`plugins/pipeloom-polars/tests/test_contract.py` has a complete `harness` function.

```python
# my_engine_tests.py
from pathlib import Path

from pipeloom.builtins.filesystem import FilesystemCatalog
from pipeloom.testing.contract.engine import EngineHarness


def harness(tmp_path: Path) -> EngineHarness:
    return EngineHarness(
        engine=MyEngine(),
        catalog=FilesystemCatalog(tmp_path / "wh"),
        ...,
    )
```

## 4. Run the kit

The kit is a pytest plugin that ships in the core and does nothing until you opt in. In a
project that has pipeloom, pytest and your engine installed:

```console
$ pytest -p pipeloom.testing.pytest_plugin --pipeloom-harness my_engine_tests:harness
pipeloom conformance kit 1.0 (pipeloom 1.0.0), harness my_engine_tests:harness
```

- `--pipeloom-harness MODULE:CALLABLE` selects the engine suite. Paths on the command line are
  replaced by the kit; `-k`, `-x` and the other pytest options still work.
- The first report line carries the kit version. Keep it in your CI logs: it tells a reader which
  cells your engine passed. It changes when cells change meaning (major) or are added (minor),
  independently of the pipeloom version.
- A cell for a capability you did not declare is reported as skipped, with the reason.

Run it in CI from a directory outside your checkout, with the built wheels installed in a fresh
venv. That shows the published package passes, not just your source tree. This repository does
exactly that for polars.

To add cells of your own, subclass `EngineContract` in a test module and override
`engine_factory`, as the in-tree plugins do. That entry point keeps working beside the plugin.
`StateStoreContract` is the equivalent suite for state stores, subclassed the same way.
