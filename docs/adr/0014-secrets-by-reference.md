# ADR-0014: Secrets by reference

Architecture: [Section 14, Security and secrets](../architecture.md#14-security-and-secrets)

## Context

YAML files, manifests, events and artifacts are committed, logged and shared. Secret values must never end up in them.

## Options

- **Literal secrets in YAML.** Convenient, and unsafe.
- **Environment variables only.** Safe, but no integration with platform secret stores.
- **References resolved at runtime through providers.**

## Decision

Secrets by reference.

- Fields whose names match secret patterns accept only `${secret:...}` or `${env:...}`. A literal is PLS104.
- `${secret:<provider>/<path>}` resolves at runtime inside the executing process through the `pipeloom.secret_providers` group. Providers: `env` and `file` (core, mode-checked), `databricks` (secret scopes), `airflow` (connections and variables).
- The manifest, events and artifacts store only the references. Resolved values are registered with the redaction filter (ADR-0010).
- Discovery never imports plugins until a config names them. `PIPELOOM_DISABLE_PLUGIN_AUTOLOAD` exists for locked-down hosts.
- Grants are declared in YAML and applied by the catalog. pipeloom never escalates privileges.
- Supply chain: trusted publishing (OIDC) with PEP 740 attestations, actions pinned by commit SHA, no `pull_request_target` workflows, integration secrets only in protected environments, and private vulnerability reporting through `SECURITY.md`.

## Consequences

- The compile-time manifest is safe to share. Secrets exist only in the running process.
- `${secret:...}` is not resolved at compile time, so a bad reference shows up at run time (or in `--preflight`).

## Status

Accepted

## Date

2026-10-29
