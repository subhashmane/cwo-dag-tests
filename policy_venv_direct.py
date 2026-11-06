"""Control: PythonVirtualenvOperator is on the CWO disabled list.

Expected: import error "PythonVirtualenvOperator is not allowed".
"""

from __future__ import annotations

from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import DAG


def _hello():
    import sys

    print("venv python:", sys.executable)


with DAG(dag_id="policy_venv_direct", schedule=None, catchup=False, tags=["cwo-test", "policy"]):
    PythonVirtualenvOperator(task_id="venv_direct", python_callable=_hello, requirements=[])
