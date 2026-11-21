# ADR-0055: Canonical dataset identity

## Context

Three places built dataset names on their own: the unit graph (`assets`), the Airflow plugin (asset URI template) and the OpenLineage sink (namespace and name, with its own path parsing). The 1.4 Dagster plugin needs the same names for its asset keys. Two copies of a naming rule drift, and a drift renames a dataset in a user's lineage graph.

## Options

- **Keep one copy per integration.** No change, but each new integration adds a copy.
- **One pure function in the core.** Every integration calls it. The core stays free of plugin imports.
- **Rename datasets to a new scheme.** Cleaner names, but it breaks every lineage graph and Airflow asset dependency built on 1.0.

## Decision

The rule moves to `pipeloom.plan.identity` and every integration calls it. The module is pure: no disk, no engine, no plugin.

- `asset_uri(template, ...)` fills the Airflow URI template, percent-encoding each value. The unit graph uses it with the default template through `default_asset_uri`.
- `relation_name`, `relation_dataset`, `source_dataset` and `location_dataset` give the OpenLineage namespace and name of a model relation or a source.
- `unit_inputs`, `input_assets` and `output_assets` name what a unit reads and writes. Ephemeral models are inlined in the unit, so an ephemeral input resolves to the persisted models and sources behind it.

Names stay byte-identical to 1.0.0. A hash suffix applies only to a name 1.0 produced but the consumer rejects. The only such case is an asset URI above 1500 characters, which Airflow refuses: it is cut to 1491 characters and ends in `_` and 8 hex of the sha256 of the full URI. No other 1.0 name changes, and 1.0 rejected no OpenLineage name.

The unit graph gains no `input_assets` or `output_assets` fields in this release. Its `assets` already lists the output URIs, and the Python functions serve the Dagster plugin. Adding the fields later is additive.

A golden computed from the 1.0.0 code (`tests/goldens/identity/dataset-names-1.0.json`) covers the shop fixture and the TPC-H polars, pandas and Spark examples: Airflow asset URIs with the default and an all-placeholder template, unit graph assets, and OpenLineage inputs and outputs with and without a dataset namespace.

Provenance: designed from the OpenLineage naming spec and the Airflow asset documentation, and from the behavior of pipeloom 1.0.0. No code from other projects was used.

## Consequences

One edit changes a dataset name everywhere. The golden makes that edit visible in review. Within 1.x the module only grows. Airflow `naming.asset_uri` is gone from the plugin; it was internal.

## Status

Accepted

## Date

2026-11-20
