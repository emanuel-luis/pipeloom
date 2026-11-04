# ADR-0034: Core orchestration hooks

## Context

pipeloom 0.5 adds an orchestrator plugin that deploys a plan as a Databricks job. The core must stay free of any orchestrator, yet the plugin needs four things from it: a place for deploy options that the plan carries without changing its identity, a manifest that is byte-identical across compiles so a deployed copy can be compared, a way to add CLI commands, and secrets that resolve at run time without entering any recorded output. ADR-0004 (discovery), ADR-0010 (events and artifacts), ADR-0013 (run identity) and ADR-0014 (secrets by reference) each say something about these, and this ADR amends them. It covers the first half of the 0.5 core change; the finalize command, the orchestrator id and the link fallback for `write_json_once` follow in a second half.

## Options

- **Deploy options as a model key or a new top-level section.** Compute is a runtime setting a target may override, so a separate section would need its own precedence rules.
- **A `deploy` runtime key bound against the existing `renderers` plugin group.** One precedence rule (target over top level), one options model per plugin, no new group of specs.
- **Resolve `${secret:...}` at load time.** Simple, but the value would reach the plan, the manifest and compile output.
- **Resolve it when the options of a plugin are bound for execution.** The value lives in the plugin object only.
- **Commands through a new plugin spec group.** A command is a click object, not a PluginSpec, so a spec would add nothing.

## Decision

`deploy` is a runtime key (`RuntimeSettings.deploy`, a plugin block). Binding checks it against the `renderers` group: the kind must exist, and its options must validate against the spec's `config_model`, with line marks. The core never calls a renderer factory. The validated options, `kind` included, are recorded as `PlanMetadata.deploy` and written to the manifest under `metadata.deploy`, which is left out when the target has no deploy block. It is not in `_HASHED_METADATA`, so editing it leaves the plan hash unchanged. The JSON Schema shows `deploy` as a `oneOf` on kind, like the other plugin blocks.

`manifest_bytes(plan, stable=True)` blanks `generated_at` and `invocation_id` and empties the selection (`select`, `exclude`, `selected`), so one project and target always give the same bytes. `write_manifest` takes the same flag.

`pipeloom.commands` is a ninth entry-point group. Its entries name a click command and are discovered from metadata only: the module is imported when the command is invoked, never to list it, and `--help` lists plugin commands without importing them. A built-in command beats an entry point of the same name, and of several distributions providing one name the first by distribution name wins; both cases print a warning to stderr. A command that fails to import becomes a stub that prints the error and exits 1, so the rest of the CLI keeps working. `PIPELOOM_DISABLE_PLUGIN_AUTOLOAD` hides every plugin command. `plugins list --group commands` lists them, and `--check` imports them. The group has no PluginSpec, so it is not part of `GROUPS`, only of the `Group` literal.

`${secret:provider/path}` references resolve only when a command executes, while the options of the plugins it builds are bound (`run` now; `finalize` with the second half). `BoundPlugin.create(secrets)` re-validates the options as written with each reference replaced, and the resolved value is registered with the redactor before any plugin that holds it is built. Providers come from the `secret_providers` group, are built once per invocation with default options, and a failure of any kind is PLX109 naming the provider and path, never a value. Compile, validate and the manifest only ever see the reference text. A resolved value is masked everywhere text leaves the process: the event bus, `run_results.json` (finalize), per-unit attempt files and state store rows (the runner), and the message of an internal error. Under `boundary: task` each unit process resolves and masks on its own. A plugin factory that fails while it holds resolved options is reported as PLX109 with a masked message and no chained traceback. The naming plugin is built at compile time and therefore never resolves secrets.

Only a reference written literally in the YAML resolves. The loader records the span of each one, also under secret-like keys, whose `${env:...}` and `${var:...}` are otherwise left alone. `$${secret:provider/path}` is the escape: it renders as the text `${secret:provider/path}`, is never resolved and never asks for its provider, so an unknown provider is not an error there. Text that arrives through `${env:...}` or `${var:...}` and happens to look like a reference never resolves either. The built-in `env` provider reads the process environment, not the `env` mapping given to the API, so through the API `${secret:env/X}` and `${env:X}` can read different environments; the CLI passes the process environment to both.

The plugin API stays (0, 4). Every change is additive for plugins: a new runtime key, a new entry-point group read only by the CLI, and `BoundPlugin.create` gaining an optional argument that plugins never call.

## Consequences

An orchestrator plugin can carry its options in the manifest without touching the plan hash, and can ship a manifest whose bytes do not depend on when it was compiled. A project with a `deploy` block fails with PLR204 where the renderer plugin is not installed, which is the point. Masking is by exact value: a plugin that transforms a secret (encodes or truncates it) before logging it defeats the redactor, and values shorter than four characters are never masked. A `${secret:` reference in a place the core does not bind as plugin options (model SQL, vars) stays literal.

## Status

Accepted.

## Date

2026-11-04
