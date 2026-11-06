"""Evidence for the two policy findings. Every result is returned through XCom.

baseline_image_has_cowsay  Is cowsay already in the CWO image? Expected False.
baseline_task_pip          Can an ordinary @task pip-install? Sets the impact bar.
venv_subclass_proof        Disabled operator under another class name: where does
                           cowsay import from, and which interpreter ran?
sidecar_image_proof        Plain @task, pod_override sidecar: prove the sidecar is
                           really busybox and record its uid.
"""

from __future__ import annotations

from kubernetes.client import models as k8s

from airflow.providers.standard.operators.python import PythonVirtualenvOperator
from airflow.sdk import dag, task


class TeamVenvOperator(PythonVirtualenvOperator):
    pass


def _venv_proof() -> dict:
    import sys

    import cowsay

    return {
        "sys_executable": sys.executable,
        "sys_prefix": sys.prefix,
        "base_prefix": sys.base_prefix,
        "cowsay_file": cowsay.__file__,
        "cowsay_version": getattr(cowsay, "__version__", "unknown"),
    }


SHARED = k8s.V1VolumeMount(name="shared", mount_path="/shared")
POD_OVERRIDE = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(name="base", volume_mounts=[SHARED]),
            k8s.V1Container(
                name="side",
                image="docker.io/library/busybox:1.36",
                command=[
                    "sh",
                    "-c",
                    "{ busybox | head -1; echo uid=$(id -u) gid=$(id -g); ls -l /bin/busybox; } > /shared/proof.txt 2>&1; sleep 30",
                ],
                volume_mounts=[SHARED],
            ),
        ],
        volumes=[k8s.V1Volume(name="shared", empty_dir=k8s.V1EmptyDirVolumeSource())],
    )
)


@dag(schedule=None, catchup=False, tags=["cwo-test", "policy", "evidence"])
def verify_findings():
    @task
    def baseline_image_has_cowsay() -> dict:
        import importlib.util
        import sys

        return {"cowsay_in_image": importlib.util.find_spec("cowsay") is not None, "python": sys.executable}

    @task
    def baseline_task_pip() -> dict:
        import subprocess
        import sys

        r = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--no-cache-dir", "--target", "/tmp/pipcheck", "cowsay==6.1"],
            capture_output=True,
            text=True,
            timeout=180,
        )
        return {"returncode": r.returncode, "tail": (r.stdout + r.stderr)[-600:]}

    venv = TeamVenvOperator(
        task_id="venv_subclass_proof",
        python_callable=_venv_proof,
        requirements=["cowsay==6.1"],
        system_site_packages=False,
    )

    @task(executor_config={"pod_override": POD_OVERRIDE})
    def sidecar_image_proof() -> str:
        import pathlib
        import time

        p = pathlib.Path("/shared/proof.txt")
        for _ in range(45):
            if p.exists() and p.stat().st_size:
                time.sleep(1)
                return p.read_text()[:1500]
            time.sleep(2)
        return "sidecar never wrote /shared/proof.txt"

    baseline_image_has_cowsay()
    baseline_task_pip()
    venv
    sidecar_image_proof()


verify_findings()
