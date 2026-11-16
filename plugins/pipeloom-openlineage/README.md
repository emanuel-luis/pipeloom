# pipeloom-openlineage

An [OpenLineage](https://openlineage.io) event sink for
[pipeloom](https://github.com/emanuel-luis/pipeloom). It turns run and unit events into
OpenLineage run events and sends them with the openlineage-python client, to Marquez or any other
OpenLineage backend.

```console
$ pip install "pipeloom[openlineage]"
```

Add it to the project's sinks:

```yaml
events:
  sinks:
    - kind: openlineage
      buffered: true
      transport:
        type: http
        url: http://localhost:5000
```

- The pipeloom run is the OpenLineage job `<project>`. Each persisted model is a child job
  `<project>.<model>` with a parent run facet that points at the pipeline run.
- Run ids are uuid5 values derived from the pipeloom run id, the unit id and the attempt, so every
  process of one run (a unit task, `pipeloom finalize`) reports to the same OpenLineage runs.
- Inputs and outputs come from the manifest: catalog relations and source locations.
- Facets: `schema`, `outputStatistics`, `dataQualityAssertions`, `errorMessage`, `parent`,
  `jobType` and `sourceCodeLocation`.
- Options: `namespace` (default `pipeloom.{target}`), `dataset_namespace`, `transport` (an
  openlineage-python transport config; without it the client reads `OPENLINEAGE_URL` or
  `openlineage.yml`) and `inside_airflow` (`skip`, the default, or `emit`). Inside an Airflow task
  the Airflow OpenLineage provider reports the lineage, so the sink stays silent unless
  `inside_airflow: emit`.

Documentation: https://emanuel-luis.github.io/pipeloom/latest/

License: Apache-2.0.
