"""@task.kubernetes is backed by KubernetesPodOperator (CAF-517).

Expected on a build with the CAF-517 fix: import error
"_KubernetesDecoratedOperator is not allowed".
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["cwo-test", "policy"])
def policy_k8s_decorator():
    @task.kubernetes(image="docker.io/library/python:3.12-slim", name="k8s-decorator", in_cluster=True)
    def in_pod() -> int:
        return 42

    in_pod()


policy_k8s_decorator()
