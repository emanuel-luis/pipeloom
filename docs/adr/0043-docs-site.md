# ADR-0043: Docs site

Architecture: [Section 1, Principles and non-goals](../architecture.md); [Section 3, Architecture overview](../architecture.md)

## Context

pipeloom writes everything a browser needs to show a project: `manifest.json` (nodes, sources, edges, bodies, resolved configs) and `run_results.json` (status, rows, bytes and errors per model). The only way to see the graph is `pipeloom graph --output mermaid`, and nothing lets a reader browse models. dbt solves this with `dbt docs generate` and `dbt docs serve`.

The architecture lists "a scheduler service or UI" as one non-goal. The roadmap splits it: a scheduler, or a UI that writes, needs authentication, a database and a daemon, while a read-only view over files pipeloom already writes needs none of that. The roadmap also planned the graph with "one pinned and vendored JavaScript library". This ADR decides that, and the other points that shape the site.

## Options

- **Non-goal scope.**
  - Keep all UI out. Nothing changes, but the observability work stays invisible and users keep writing their own viewers.
  - Narrow it to "a scheduler service, or any UI that starts, schedules or changes runs". A read-only viewer is allowed and the risky half stays out.
- **Site shape.**
  - Multi-file: an HTML shell that fetches `manifest.json` at load. Smaller HTML, but `fetch` on `file://` fails in some browsers, so it needs a server.
  - One self-contained `index.html`. It opens from disk, by double click or as an attachment.
- **Graph.**
  - A vendored library: cytoscape.js (about 400 KB, no dependencies) or mermaid (about 3 MB with bundled dependencies). Interactive, but it adds license and update work, makes the output hard to test byte for byte, and grows the core wheel.
  - A layout computed in Python and written as inline SVG. Static, but deterministic, offline by construction and testable with a golden file.
- **Templating.**
  - Jinja. Familiar, but a new dependency in a core that bans Jinja for models and keeps a short dependency list.
  - The standard library: `html.escape` and string building.
- **Input.**
  - Always compile the project. Simple, but it needs the project's plugins and environment.
  - Also accept `--manifest PATH`, so CI can render a manifest produced elsewhere.

## Decision

- The non-goal becomes "a scheduler service, or any UI that starts, schedules or changes runs". `pipeloom docs generate` and `pipeloom docs serve` are read only.
- The site is one `index.html`, written to `--out` (default `<project-dir>/target/docs`), which holds exactly that file. All CSS and the little JavaScript are inline. A `Content-Security-Policy` meta tag (`default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; img-src data:`) stops the page from loading anything external. Colors follow `prefers-color-scheme`.
- The HTML is rendered in Python with the standard library, and `manifest.json` is not embedded: the HTML is the rendering, and the manifest stays the data contract. The output is deterministic. It holds no wall-clock time; it shows the manifest's `generated_at` when that is not blank.
- The graph deviates from the roadmap's vendored library. It is an inline SVG laid out by a pure Python module (`pipeloom.docs.layout`): layer = longest path from the roots, order inside a layer from (path, name) then four alternating barycenter sweeps with stable tie-breaks, fixed node size, each layer centered, and one cubic Bezier per edge with no dummy nodes. Reasons: no license or update work, a golden-testable output, offline by construction, and a smaller core wheel. A few lines of vanilla JavaScript add zoom buttons and a model filter; with JavaScript off the page still works. The interactive graph is left to `pipeloom ui` (1.1), which ships as its own distribution.
- Content: a header (project, target, version, short plan hash and, with results, the last run), the graph, a filterable model table, one section per model (relation, materialization, strategy, tags, inputs, parents, children, expectations, resolved config, body, last result with the `pipeloom explain CODE` hint) and one section per source. Model contracts (`columns`) arrive in 0.9 and are shown then.
- Inputs: by default the command compiles like `pipeloom compile` (`--project-dir`, `--target`, `--vars`, `--plugin`, `--log-format`), reports diagnostics on stderr and exits 1 on errors. `--manifest PATH` renders an existing manifest (PLX107 and exit 1 when unreadable). Results come from `--run-results PATH`, else from the `run_results.json` at the root of the artifacts root when it exists; with `--manifest` and no flag there are none. A results file that is not `pipeloom.run_results/1`, or belongs to another project, is a usage error (exit 2). Results with a different `plan_hash` still show per `unique_id`, with a note in the header. No diagnostic code is added.
- `pipeloom docs serve` runs the standard library `ThreadingHTTPServer` over `--dir` (default `<project-dir>/target/docs`) on `--host` (default `127.0.0.1`) and `--port` (default 8000). A missing `index.html` is a usage error. It never opens a browser and stops cleanly on Ctrl+C. `make_server` lives in `pipeloom.docs.serve` so tests can run it on port 0.
- `pipeloom.docs` is a core package above `plan`, `artifacts`, `graph` and `diagnostics` in the import-linter layers, beside `runtime`: it imports neither `runtime` nor a plugin.

## Consequences

- The core wheel gains no dependency and no bundled asset. The site works from `file://`, in an air-gapped network, and as a CI artifact.
- The graph cannot pan, zoom to a node or collapse groups. Large projects scroll. Layout quality is that of a simple layered drawing, and long edges can cross nodes; `pipeloom ui` is the place for an interactive graph.
- Output is byte-stable for a given manifest and results, so the SVG of the shop fixture is a golden file and layout changes show up in review.
- The page grows with the project: every model's SQL and config is in the HTML. A project with thousands of models gives a large file.
- Widening the read-only exception to `pipeloom ui` still needs its own ADR for the server choice.

## Status

Accepted.
