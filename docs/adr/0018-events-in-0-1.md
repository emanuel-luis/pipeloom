# ADR-0018: Events in 0.1: invocation events, nullable run id, jsonl on stderr

Architecture: [Section 11](../architecture.md#11-events-logs-and-run-metadata)

Amends [ADR-0010](0010-events-sinks-artifacts.md).

## Context

ADR-0010 defines the event stream for runs. In 0.1 there is no run yet, but every command (`validate`, `schema export`, `plugins list`, later `compile`) should report through the same bus, so diagnostics, logs and artifacts do not grow a second path. A few details of the envelope also need fixing before any consumer depends on them.

## Options

- **Keep printing diagnostics directly and add events with runs in 0.2.** Two paths to maintain, and the console format would change later.
- **Route every command through the bus now, with a small set of invocation events.**

## Decision

Every command is an invocation with its own `invocation_id` and an event bus.

- `run_id` is nullable. Outside a run it is `null`.
- New event types: `invocation.started`, `invocation.completed` and `plan.compiled`. `plan.compiled` is the compile-only form of `run.compiled` (project, target, plan hash, unit counts, selection). `diagnostic`, `artifact.written` and `sink.disabled` are used as in ADR-0010.
- The envelope has a `schema` key with the value `pipeloom.event/1`, so a line of jsonl identifies itself.
- `data` is always a JSON object.
- A command collects what it finds and emits at the end, in this order: `invocation.started`, diagnostics (sorted by file, line, column, code), `plan.compiled` and `artifact.written` if any, `invocation.completed`.
- `--log-format text|json` (`PIPELOOM_LOG_FORMAT`) picks the terminal sink: `console` (text) or `jsonl` to stderr (json). Sinks listed under `events.sinks` in `pipeloom.yml` are added after the project loads and binds. Stdout stays for command results.
- `console` and `jsonl` are built-in sink plugins. `failure_budget`, `buffered` and `buffer_size` apply to every sink. The `jsonl` sink takes an optional `path`, relative to the project directory; without it the sink writes to stderr.
- A sink that fails more than its budget in a row is disabled (PLE401, a warning) and the other sinks receive `sink.disabled`.

## Consequences

- Scripts read machine output from stderr with `--log-format json` and command results from stdout.
- Diagnostics are visible in events, so later sinks (files, lineage) get them without extra work.
- Nothing is printed until a command ends, so a long command shows no progress. Runs in 0.2 emit as they go.
- Redaction is in the bus, but nothing registers secret values until providers resolve secrets at run time.

## Status

Accepted

## Date

2026-11-04
