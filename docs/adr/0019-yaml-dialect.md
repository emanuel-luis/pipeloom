# ADR-0019: YAML dialect: PyYAML with YAML 1.2 core scalars

Architecture: [Section 4](../architecture.md#4-configuration)

## Context

Every config file is YAML, and users hit its sharp edges. YAML 1.1, which PyYAML implements by default, turns `NO` into a boolean (the Norway problem), reads `010` as octal and resolves bare dates to timestamps. Diagnostics also need the line and column of every key and value, and files can be hostile: duplicate keys, recursive aliases and alias expansion bombs must not hang or crash the tool.

## Options

- **PyYAML as is.** Zero extra work, but YAML 1.1 scalars silently change country codes and ids, and duplicate keys overwrite each other.
- **ruamel.yaml.** Tracks marks and speaks YAML 1.2, but adds a dependency with its own quirks, and its round-trip data types leak into the models.
- **PyYAML with a YAML 1.2 core resolver and our own constructor.** Keeps one small, well-known dependency and controls scalars, marks and limits.

## Decision

pipeloom parses with PyYAML and composes the node tree with a loader whose implicit resolvers are the YAML 1.2 core schema: only `true` and `false` are booleans, `null`, `~` and an empty value are null, integers may be decimal, `0o` octal or `0x` hex, and nothing resolves to a timestamp. `country: NO` stays a string.

Our own constructor walks the nodes. It records the mark of every key and value under its RFC 6901 pointer, reports duplicate keys as PLY002, supports merge keys (`<<`, with explicit keys winning and merged keys exempt from the duplicate check) and rejects non-string keys. It counts every constructed node, including those reached through aliases. A recursive alias, or an expansion above 100000 nodes, is PLY001. More than one document in a file is PLY001. Bytes that are not UTF-8 (a UTF-8 BOM is stripped) and characters YAML forbids are PLY003.

## Consequences

- Configs behave the same on every machine, and Norwegian users can write `NO`.
- Files that rely on YAML 1.1 booleans such as `yes` or `on` get strings, and pydantic's lax mode only converts the strings it knows.
- Diagnostics point at exact lines and columns. A value that comes from an alias reports the mark of the anchor's definition.
- The loader is a small amount of code we own and test. A move to another parser later only has to keep the `LoadedDocument` contract.

## Status

Accepted

## Date

2026-11-04
