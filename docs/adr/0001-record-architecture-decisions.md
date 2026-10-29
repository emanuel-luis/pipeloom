# ADR-0001: Record architecture decisions

Architecture: Where the decision is recorded: `docs/adr/` (this folder).

## Context

pipeloom is designed before it is built, and plugin authors and contributors need to see why each choice was made, not only what it is. Decisions made in chat or in a pull request thread get lost. The architecture document ([architecture.md](../architecture.md)) holds the full design, but a single long document is hard to amend one decision at a time.

## Options

- **Decisions only in the architecture document.** One place to read. Changing one decision means editing a long page, and the reason a choice was made drifts away from its date.
- **A wiki or issue tracker.** Easy to edit, but it lives outside the repository, so it is not versioned with the code and not reviewed in pull requests.
- **One ADR file per decision in `docs/adr/`.** Versioned and reviewed with the code. Short files with a fixed shape. Needs a little discipline to keep the index current.

## Decision

Record each significant decision as one ADR file in `docs/adr/`, named `NNNN-short-slug.md`, with the sections Context, Options, Decision, Consequences, Status and Date (see [0000-template.md](0000-template.md)). Numbers are never reused. A decision that changes gets a new ADR that supersedes the old one, and the old ADR's status changes to Superseded. [README.md](README.md) is the index.

## Consequences

- Every decision has a stable link that issues, pull requests and docs can cite.
- A new ADR goes through the same review as code.
- The index must be updated with each new ADR.
- ADR-0001 to ADR-0016 record the decisions of the initial design (2026-10-29).

## Status

Accepted

## Date

2026-10-29
