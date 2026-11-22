### Fixed

- The Delta state store treats a concurrent-modification conflict that outlives its retries on a watermark or interval ledger compare-and-set as a lost race, so the caller reads again. Concurrent interval ledger writers on Delta no longer fail with `ConcurrentAppendException`.
