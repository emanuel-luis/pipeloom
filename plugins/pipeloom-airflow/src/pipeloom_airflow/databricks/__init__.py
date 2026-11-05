"""DATABRICKS mode (ADR-0039): a launch task starts or continues the job run, one sensor per
Databricks task mirrors its result, and a sensor on the job's finalize task ends the DAG run.

`connection`, `mirror` and `launch_logic` are pure; `launch`, `sensor` and `triggers` import
Airflow through `pipeloom_airflow._compat`.
"""
