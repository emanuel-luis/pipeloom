# ADR-0013: Run identity, idempotency, reruns

Architecture: [Section 17, ADR-0013 text](../architecture.md#17-adr-list-and-diagrams)

## Context

A run can span several processes and orchestrator tasks, and tasks can be retried or repaired. Metadata, events and writes from all of them must join into one run, and a retry must not duplicate data.

## Options

- **A fresh `run_id` per attempt.** Simple, but splits one logical run into many.
- **A stable `run_id` with an attempt counter,** derived deterministically by orchestrators.

## Decision

- `run_id` is either caller-supplied or generated as `YYYYMMDDTHHMMSSZ-<6 hex>`, matching `[A-Za-z0-9._-]{1,64}`.
- Orchestrators derive it deterministically: `dbx-<job_run_id>` or `af-<hash>`.
- Orchestrator retries and repairs keep the `run_id` and increment the `attempt`.
- Local `pipeloom retry --from <run_id>` creates a new run with `parent_run_id`, selecting `result:fail+` (failed units plus their skipped descendants).
- Writes are idempotent through the token `<run_id>:<model>`.

## Consequences

- A cleared-and-rerun Airflow task continues the same pipeloom run.
- Engines without idempotent writes declare `idempotent_writes=False`, and validation forbids retries on non-idempotent append (PLC505).
- Run records go to the state store for history.

## Status

Accepted

## Date

2026-10-29
