### Added

- `pipeloom backfill --select --start --end`: runs interval models over an explicit range, rounded outward to window edges. `--rerun missing` (default) runs only the windows the ledger lacks; `--rerun all` restates the range, removing it from the ledger first so a stopped restate resumes as missing; `--on-failure continue` runs every window and reports each failed one; `--mark-done` seeds the ledger without running anything ([ADR-0054](docs/adr/0054-interval-incrementals.md#backfill-amendment-13), [guide](docs/guide/incremental.md#backfilling-a-range)).
- The optional `failed` field on `intervals` entries of run_results, and `backfill` in the run's `args`. `pipeloom retry` of a backfill run keeps its range.
- Diagnostic code PLX132 (nothing to backfill).

### Documentation

- New guide: Migrating within 1.x, converting a watermark model to an interval model with `--mark-done`.
