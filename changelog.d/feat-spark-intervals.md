### Added

- Spark: interval incrementals on Delta. The engine declares `interval_writes`, replaces each window with one `replaceWhere` commit over the event_time range, and filters the window's input with a SQL predicate ([ADR-0032 amendment 1.3](docs/adr/0032-delta-writes-and-idempotency.md)).

### Changed

- Spark: a window write commits under its own Delta transaction app id, `pipeloom/<project>/<model>/<interval_id>`, so windows of one run never replay each other.

### Documentation

- Spark plugin page: interval writes in the capabilities table and a note on window writes.
