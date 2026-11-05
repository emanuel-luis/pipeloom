"""Databricks renderer for pipeloom: a compiled plan as Jobs settings JSON and bundle YAML.

Rendering is pure. Nothing here imports `databricks.sdk`; only the modules that talk to a
workspace will, and they load it lazily.
"""

from __future__ import annotations


def pipeloom_databricks_task() -> None:
    """The entry point of a rendered task, found by `package_name` and `entry_point`.

    A function of the package covers a Databricks lookup that does not read entry points. The
    task module loads only when a task runs.
    """
    from pipeloom_databricks.task import main

    main()
