# Docs site

`pipeloom docs generate` renders your project as one HTML file. It shows the lineage graph, every model and
source, and the result of the last run. The file has no external resources, so you can open it from disk, attach it
to a ticket or host it on any static server. The site is read only: it never starts, schedules or changes a run.

```text
$ pipeloom docs generate
target/docs/index.html
$ pipeloom docs serve
serving target/docs at http://127.0.0.1:8000/
```

## What the page shows

- The header: project, target, pipeloom version and a short plan hash. With results, also the last run's id, status,
  end time and counts.
- The graph, drawn left to right. Sources and models are boxes; ephemeral models have a dashed border. A box is
  colored by the model's last status (succeeded, failed, skipped, cancelled, timed out, or none). Click a box to jump
  to its section. The `+`, `-` and `100%` buttons zoom the graph.
- A table of models: name, path, materialization, write strategy, relation, tags, last status, rows written and
  duration. The filter box above it narrows the rows as you type.
- One section per model: relation, materialization, strategy, tags, inputs (each linked to the model or source it
  reads), parents and children, expectations, the resolved config as JSON, and the body (the SQL text, or the Python
  callable and its file). With results it adds status, rows, bytes, duration, attempt and, for a failure, the error
  code and message with the `pipeloom explain CODE` command that describes it.
- One section per source.

The zoom buttons and the filter box need JavaScript. With JavaScript off, everything else still works.

## Generate

`pipeloom docs generate` compiles the project the way `pipeloom compile` does, so it takes `--project-dir`,
`--target`, `--vars`, `--plugin` and `--log-format`. Diagnostics go to stderr, and any error exits 1. It prints the
path of the file it wrote on stdout.

| Flag | Meaning |
|---|---|
| `--out DIR` | Output folder, relative to the project dir unless absolute. Default `target/docs`. The folder gets one file, `index.html`. |
| `--manifest PATH` | Render this manifest instead of compiling. A manifest that cannot be read is PLX107 and exit 1. |
| `--run-results PATH` | Show this `run_results.json`. The file must exist. |

Without `--run-results`, the page uses the `run_results.json` at the root of the target's artifacts root
(`target/run_results.json` by default), if it exists. With `--manifest` and no `--run-results`, the page has no
results. A results file that is not a `pipeloom.run_results/1` document, or that belongs to another project, is a
usage error (exit 2) that names the file.

When the results come from a different plan than the one shown, the page still matches rows to models by id and adds
a note that the plan changed since that run.

The output depends only on its inputs, so the same manifest and results give the same bytes. The page shows the
manifest's `generated_at` unless it is blank, as in `pipeloom compile --stable`.

## Serve

`pipeloom docs serve` serves the folder with the Python standard library. It never opens a browser.

| Flag | Meaning |
|---|---|
| `--project-dir DIR` | Directory that holds `pipeloom.yml`. |
| `--dir DIR` | Folder to serve, relative to the project dir unless absolute. Default `target/docs`. |
| `--host HOST` | Address to listen on. Default `127.0.0.1`. |
| `--port N` | Port. Default 8000; 0 picks a free one. |

If `index.html` is missing, the command exits 2 and asks you to run `pipeloom docs generate` first. Press Ctrl+C to
stop it; it exits 0.

## Limits

The graph is a static drawing, not an interactive one. Pan and zoom are the scroll area and the buttons. An
interactive viewer over the artifacts root is planned as `pipeloom ui`, in its own distribution. Model contracts
(`columns`) are not shown yet.
