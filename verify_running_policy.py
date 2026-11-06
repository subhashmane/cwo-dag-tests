"""Return the cluster policy that is actually deployed, so findings are tied to
the running code and config rather than a repo tag.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["cwo-test", "evidence"])
def verify_running_policy():
    @task
    def running_policy() -> dict:
        import hashlib
        import inspect
        import os

        import airflow_local_settings as als

        path = inspect.getsourcefile(als)
        src = open(path, "rb").read()
        cfg_path = f"{os.environ.get('AIRFLOW_HOME')}/cwo_config.cfg"
        cfg = open(cfg_path).read() if os.path.exists(cfg_path) else "missing"
        return {
            "module_path": path,
            "sha256": hashlib.sha256(src).hexdigest(),
            "task_policy_source": inspect.getsource(als.task_policy),
            "cwo_config": cfg[:1500],
        }

    running_policy()


verify_running_policy()
