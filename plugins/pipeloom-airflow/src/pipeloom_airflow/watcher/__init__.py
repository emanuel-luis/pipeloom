"""WATCHER mode (ADR-0048): one producer task runs the selection, one watcher per unit reads its
files. `state` is pure and imports no Airflow; import from the submodules."""
