"""Subclass of a disabled operator with a different class name.

If the CWO task policy only matches exact class names, this DAG loads and the
task builds a virtualenv and pip-installs a package the image does not ship.
"""

from __future__ import annotations

from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import DAG


class TeamVenvOperator(PythonVirtualenvOperator):
    pass


def _prove_install():
    import sys

    import cowsay

    print("venv python:", sys.executable)
    print("cowsay from:", cowsay.__file__)
    cowsay.cow("installed inside a disabled operator")


with DAG(dag_id="policy_venv_subclass", schedule=None, catchup=False, tags=["cwo-test", "policy"]):
    TeamVenvOperator(
        task_id="venv_subclass",
        python_callable=_prove_install,
        requirements=["cowsay==6.1"],
        system_site_packages=False,
    )
