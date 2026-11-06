"""Probe the remote-logging path with CWO's own `s3_logs` connection.

Results go to XCom because task logs may not be readable from the UI.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["cwo-test", "logging"])
def s3_log_probe():
    @task
    def probe() -> dict:
        import os

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        out: dict = {"env_remote_base": os.environ.get("AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER")}
        hook = S3Hook(aws_conn_id="s3_logs")
        try:
            out["identity"] = hook.get_session().client("sts").get_caller_identity()["Arn"]
        except Exception as exc:
            out["identity_error"] = repr(exc)[:400]
        try:
            out["keys"] = (hook.list_keys(bucket_name="sub-cwo-airflow-logs", max_items=20) or [])[:20]
        except Exception as exc:
            out["list_error"] = repr(exc)[:400]
        try:
            hook.load_string("probe", key="cwo-probe/write-test.txt", bucket_name="sub-cwo-airflow-logs", replace=True)
            out["write"] = "ok"
        except Exception as exc:
            out["write_error"] = repr(exc)[:400]
        return out

    probe()


s3_log_probe()
