"""Databricks renderer for pipeloom: a compiled plan as Jobs settings JSON and bundle YAML.

Rendering is pure. Nothing here imports `databricks.sdk`; only the modules that talk to a
workspace will, and they load it lazily.
"""
