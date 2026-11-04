"""Deliberate parse failure.

The DAG processor should record this file as an import error and keep
parsing the other files. It must not exit the process.
"""

from __future__ import annotations

from airflow.sdk import DAG

with DAG(dag_id="broken_parse", schedule=None, catchup=False, tags=["cwo-test"]):
    raise RuntimeError("deliberate parse failure for CWO import-error testing")
