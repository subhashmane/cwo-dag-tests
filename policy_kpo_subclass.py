"""Subclass of KubernetesPodOperator with a different class name.

Parse-time question: does the CWO task policy reject it?
Run-time question: can the task pod reach the Kubernetes API at all?
"""

from __future__ import annotations

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG


class TeamPodOperator(KubernetesPodOperator):
    pass


with DAG(dag_id="policy_kpo_subclass", schedule=None, catchup=False, tags=["cwo-test", "policy"]):
    TeamPodOperator(
        task_id="kpo_subclass",
        name="kpo-subclass",
        image="busybox:1.36",
        cmds=["sh", "-c", "echo pod launched by subclass; id"],
        in_cluster=True,
        get_logs=True,
    )
