### Added

- `blocking: false` on a data test: a failure still fails the unit with PLQ303 and the run exits 1, but the descendants run. Unset keeps the 1.0 behavior ([ADR-0056](docs/adr/0056-run-outcome-fields.md), [guide](docs/guide/testing.md#non-blocking-data-tests)).
- The optional `failed_upstream` field on `upstream_failed` rows of run_results: the unit whose failure skipped this one, or `<unit_id>.<data_test>` when a blocking data test failed. `pipeloom finalize` fills it too.

### Changed

- A PLQ303 message lists at most 20 failing test names and counts the rest.

### Documentation

- `skip_reason` is documented as an open set with a failing or non-failing class per value, including the reserved `not_selected`, `up_to_date`, `not_due` and `upstream_incomplete` ([reference](docs/reference/artifacts.md#skip-reasons)).
