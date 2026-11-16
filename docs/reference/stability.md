# Stability and deprecations

From 1.0.0 pipeloom follows [Semantic Versioning](https://semver.org/). The decision is in
[ADR-0016](../adr/0016-versioning-and-plugin-api.md) (amendment 1.0); the artifact schemas are
defined in [ADR-0049](../adr/0049-artifact-schemas-and-orchestrator-protocol.md).

## What you can rely on

Within a major version these do not change in a breaking way:

- CLI command names, flag names and what they mean, and the exit codes 0, 1, 2 and 130.
- JSON output that carries a version tag.
- `config_version: 1`. It is the only accepted value through 1.x.
- The artifact schemas `pipeloom.manifest/1`, `pipeloom.run_results/1`, `pipeloom.event/1`,
  `pipeloom.unit_states/1` and `pipeloom.unit_graph/1`, and the event types.
- Plugin API 1.0 for plugin authors: the protocols, `PluginSpec`, the capabilities and
  `pipeloom.testing`. A plugin works with a core that has the same major and a minor equal to or
  above the plugin's; otherwise you get PLU001.
- The meaning of each diagnostic code.
- The documented Python API: the `pipeloom` top-level exports and `pipeloom.api`.

## What may change in a minor release

- Console text, including messages and hints of diagnostics, log lines and table layout. Read
  the code or the JSON output instead.
- Anything undocumented, such as private modules and names that start with an underscore.
- Additions: new commands, flags, optional fields in artifacts, event types, diagnostic codes and
  optional plugin members. Readers of artifacts and events should ignore what they do not know.

A rule that let wrong data through can start rejecting configuration in a minor or patch release.
The CHANGELOG lists it under Security or Fixed.

## How a deprecation reaches you

1. The CHANGELOG lists the item under "Deprecated" and the table below gets a row.
2. When you use the item, pipeloom prints a warning diagnostic with a `PLV` code. For Python and
   plugin API members it also raises a Python `DeprecationWarning`.
3. The item keeps working for the rest of the major version. It is removed only in the next
   major, and only after at least one minor release that warned.

## Deprecations

| Item | Deprecated in | Replacement | Earliest removal |
| ---- | ------------- | ----------- | ---------------- |

None yet.
