### Added

- Canonical dataset identity ([ADR-0055](docs/adr/0055-canonical-dataset-identity.md)): `pipeloom.plan.identity` is the one place that names datasets. The unit graph, the Airflow assets and the OpenLineage sink call it, and the Dagster plugin will. Names are byte-identical to 1.0.0, proven by a golden for the shop fixture and the TPC-H examples. `input_assets` and `output_assets` resolve an ephemeral input to its persisted upstream. An asset URI above 1500 characters, which Airflow rejects, now ends in a hash suffix.
