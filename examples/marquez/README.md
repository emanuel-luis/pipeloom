# Marquez reference check

[Marquez](https://marquezproject.ai) is the reference backend of the `pipeloom-openlineage` sink
([ADR-0046](../../docs/adr/0046-openlineage.md)). `compose.yml` starts it locally as the compose
project `pipeloom-marquez`:

| Service | Host port | What |
| --- | --- | --- |
| `api` | 15000 | OpenLineage endpoint and Marquez API |
| `api` | 15001 | admin (health checks) |
| `web` | 13000 | web UI |
| `db` | none | Postgres, reachable only from the other services |

The check needs Docker and runs by hand before a release, not in CI. From the repository root:

```console
$ uv run examples/tpch_data.py
$ docker compose -p pipeloom-marquez -f examples/marquez/compose.yml up -d
$ curl -fs http://localhost:15000/api/v1/namespaces
$ uv run --all-packages python tools/marquez_check.py
$ docker compose -p pipeloom-marquez -f examples/marquez/compose.yml down -v
```

Repeat the `curl` until it answers: the API starts a few seconds after the database. The check
copies `examples/tpch_polars` to a temp folder, adds an `openlineage` sink that posts to
`http://localhost:15000`, runs the project with a fresh run id and then reads the Marquez API. It
prints one line per fact and exits 1 when any fails:

```text
CHECK run ok
CHECK jobs ok
CHECK lineage_edges ok
CHECK schema_fields ok
CHECK quality_assertions ok
CHECK parent_run_ids ok
```

- `jobs`: the job `tpch_polars` and one job `tpch_polars.<model>` per persisted model.
- `lineage_edges`: the inputs and outputs of every model job equal the manifest's.
- `schema_fields`: every output dataset has schema fields.
- `quality_assertions`: the outputs of the models with expectations carry assertions.
- `parent_run_ids`: every model run points at the pipeloom run as its parent.

`--keep` keeps the copy (manifest, warehouse and run artifacts) and prints its path. `--url`
points the sink and the check at another Marquez. The tool never starts or stops Docker. Open
http://localhost:13000 to browse the jobs and datasets in the namespace `pipeloom.dev`.
`down -v` also drops the database volume; leave out `-v` to keep the history between checks.
