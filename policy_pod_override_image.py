"""No disabled operator: a plain @task whose executor_config pod_override adds a
sidecar from an arbitrary public image.

The sidecar writes to a shared emptyDir; the task reads it and returns it via
XCom, so the result is visible even when task logs are not.
"""

from __future__ import annotations

from kubernetes.client import models as k8s

from airflow.sdk import dag, task

SIDECAR = k8s.V1Container(
    name="side",
    image="docker.io/library/busybox:1.36",
    command=["sh", "-c", "uname -a > /shared/proof.txt; cat /etc/os-release >> /shared/proof.txt; sleep 300"],
    volume_mounts=[k8s.V1VolumeMount(name="shared", mount_path="/shared")],
)

POD_OVERRIDE = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(name="base", volume_mounts=[k8s.V1VolumeMount(name="shared", mount_path="/shared")]),
            SIDECAR,
        ],
        volumes=[k8s.V1Volume(name="shared", empty_dir=k8s.V1EmptyDirVolumeSource())],
    )
)


@dag(schedule=None, catchup=False, tags=["cwo-test", "policy"])
def policy_pod_override_image():
    @task(executor_config={"pod_override": POD_OVERRIDE})
    def read_sidecar() -> str:
        import pathlib
        import time

        p = pathlib.Path("/shared/proof.txt")
        for _ in range(60):
            if p.exists() and p.stat().st_size:
                return p.read_text()[:2000]
            time.sleep(2)
        return "sidecar never wrote /shared/proof.txt"

    read_sidecar()


policy_pod_override_image()
