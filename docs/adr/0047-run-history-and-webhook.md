# ADR-0047: Run history and the webhook sink

## Context

Every state store already records one `RunRecord` per run, and every run writes `runs/<run_id>/run_results.json` under the artifacts root, but nothing lists them: to find the last failed run a user opens folders by hand. The roadmap (section 3, End-to-end observability) asks for `StateStore.list_runs` in the plugin API at version (0, 5), a `pipeloom runs` command over it, and a `webhook` sink that tells a chat channel or a pager when a run fails, with stdlib HTTP only.

Plugins implement `StateStore` structurally: they do not subclass the protocol, so a default method body on the protocol never reaches them. While the plugin API major is 0 the core accepts only plugins built for its exact version ([ADR-0016](0016-versioning-and-plugin-api.md), [ADR-0017](0017-plugin-spec-and-builtins.md)). The event bus already disables a sink that fails `failure_budget` times in a row and emits one `sink.disabled` event (PLE401), but it resets the count on every delivered event, and a webhook ignores most events, so a streak of failed posts is never consecutive from the bus's point of view.

## Options

- **History from the state store only.** One source, but runs whose store was lost, or that ran on another machine with a local sqlite file, cannot be listed.
- **History from run_results.json only.** Works without a store, but needs a readable artifacts root and parses one file per run.
- **Both, the store by default and the artifacts on request.** Chosen.
- **Page by timestamp.** Two runs can start in the same microsecond, and the user has to copy a timestamp.
- **Page by run id.** A run id is what `ls` prints and what `show` and `retry --from` take.
- **Webhook budget in the bus.** The bus would have to know which events a sink ignores.
- **Webhook budget in the sink.** The sink knows what it posted; the bus keeps reporting.

## Decision

**Plugin API (0, 5).** `StateStore` gains `list_runs(project, target, limit, before=None) -> tuple[RunRecord, ...]`. `API_VERSION` becomes (0, 5). During 0.x the match stays exact, so every first-party plugin moves with the core (they declare `API_VERSION`) and a plugin built for (0, 4) fails with PLU001; ADR-0016 does not allow a compatibility window before 1.0.

**Order and paging.** `list_runs` returns the runs of one project and target, newest first, ordered by `(started_at, run_id)` descending, at most `limit` (at least 1). `before` is a run id: the result starts with the run that comes right after it in that order. A `before` that names no run of that project and target raises `UnknownRun` (a `LookupError`).

**Default for stores that lack it.** The core never calls the method directly: `pipeloom.state.history.list_runs(store, ...)` calls it when the store has it and otherwise raises `ListRunsUnsupported`, which the CLI reports as PLX117 with a hint to use `--artifacts`. This is the default the roadmap asks for, placed where a structural plugin cannot skip it.

**Implementations.** A shared helper, `page_runs`, orders and pages a sequence of records, so the stores agree on ties. The sqlite store selects the project's and target's rows, the json store reads `runs/*/run.json`, and the Spark plugin's `table` store selects the `run` rows of that project and target. Each call reads the whole history of one project and target; that is fine for thousands of runs, and a store can push the order and the limit down later without changing the contract. The state store contract suite gains the ordering, paging and unknown-cursor tests, so the Spark table store is checked by the same tests in CI.

**`pipeloom runs`.** A command group with `ls` and `show RUN_ID`, read only (it takes no lock):

- `runs ls` prints the last runs of the bound target, `--limit N` (default 20), `--before RUN_ID` for the next page, `--target`, and `--output text|json` like `ls`. Text is one tab-separated line per run: run id, status, started at, duration and unit counts. JSON is a list of the state records as the json store writes them.
- `runs show RUN_ID` prints one run and its unit attempts; `--output json` prints `{"run": ..., "units": [...]}`.
- `--artifacts` on both reads `<artifacts root>/runs/*/run_results.json` of the bound target instead of the store, keeps the documents of this project and target, and skips folders without a readable `pipeloom.run_results/1` document (a run that is still running or crashed). Units then come from the `results` rows.
- An unknown run id (`show`, `--before`) is PLX118. Both codes exit 1 and go through the event bus to stderr; results go to stdout.

**The `webhook` sink.** A built-in sink, kind `webhook`, with these options:

- `secret_url`: the URL, given as `${secret:provider/path}`. The name is secret-like, so a literal URL is PLS104. Only `http` and `https` are accepted. The URL is never logged, printed or put in an error message; the bus also masks it as a resolved secret.
- `events` (default `[run.completed]`): event types to post. A `run.completed` event is posted only when its status is in `statuses` (default `[failed]`).
- `min_level` (default none): also post any event at or above this level (`warning` or `error`). It is off by default so that a failed run posts exactly once.
- `timeout` (default 10 seconds) per request, and `max_failures` (default 3).

The body is one JSON object, `{"text": ..., "event": ...}`: `text` is a one-line summary (for a failed run, the run id, the status and the first failed units with their codes) that Slack and Teams incoming webhooks render, and `event` is the full redacted event envelope for anything else. It is sent with `urllib.request` as a POST with `Content-Type: application/json`; any 2xx answer is a success. No new dependency.

The failure budget lives in the sink: a failed post raises an error that names only the HTTP status or the exception type. After `max_failures` failed posts in a row the sink stops posting and fails every later delivery with one fixed message, so the bus disables it and emits one `sink.disabled` warning (PLE401), at the latest when the bus closes. The sink is meant to run with `buffered: true`, so a slow endpoint never holds up the run. A sink built without secret resolution (commands that never run, such as `validate`) still holds the reference and posts nothing.

**Codes.** PLX117 (state store cannot list runs) and PLX118 (run not found) join a new `_ACTIVE_09` table. PLX119 stays free.

## Consequences

Run history works on every built-in store and on the Spark table store, and on any artifacts root without a store. Third-party state stores must add `list_runs` and rebuild for (0, 5); until they do, PLU001 stops them. A store that declares (0, 5) but lacks the method still runs pipelines, and `runs` reports PLX117 for it. Alerting needs no new dependency, and a broken webhook costs at most `max_failures` timeouts per invocation. Paging pushdown in the stores, filters by status, and alert templates are left for later.

## Status

Accepted.

## Date

2026-11-06
