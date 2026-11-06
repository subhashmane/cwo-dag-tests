"""Complex DAG: real HTTP connection, dynamic mapping, task groups, branching,
XCom, retries, trigger rules, and a deferrable sensor on the triggerer.

Needs connection `httpbin_conn` (http, host https://httpbin.org).
"""

from __future__ import annotations

import json
from datetime import timedelta

import pendulum

from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor
from airflow.sdk import DAG, Variable, task, task_group
from airflow.utils.trigger_rule import TriggerRule

with DAG(
    dag_id="complex_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params={"fanout": 6},
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
    tags=["cwo-test", "complex"],
):
    fetch = HttpOperator(
        task_id="fetch_uuid",
        http_conn_id="httpbin_conn",
        endpoint="uuid",
        method="GET",
        response_filter=lambda r: r.json()["uuid"],
        log_response=True,
    )

    wait = TimeDeltaSensor(task_id="defer_20s", delta=timedelta(seconds=20), deferrable=True)

    @task
    def make_batches(uuid: str, **context) -> list[dict]:
        n = int(context["params"]["fanout"])
        return [{"idx": i, "seed": f"{uuid}-{i}"} for i in range(n)]

    @task(retries=3, retry_delay=timedelta(seconds=5))
    def process(batch: dict, **context) -> int:
        if batch["idx"] == 2 and context["ti"].try_number == 1:
            raise RuntimeError("deliberate first-try failure to exercise retries")
        return len(batch["seed"]) * (batch["idx"] + 1)

    @task_group
    def summarize(values):
        @task
        def total(xs) -> int:
            return sum(xs)

        @task.branch
        def route(t: int) -> str:
            return "summarize.big" if t > 100 else "summarize.small"

        @task
        def big(t: int):
            print("big total", t)

        @task
        def small(t: int):
            print("small total", t)

        t = total(values)
        r = route(t)
        r >> [big(t), small(t)]

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def record(**context):
        Variable.set("complex_pipeline_last_run", json.dumps({"run_id": context["run_id"]}))
        print("variable written:", Variable.get("complex_pipeline_last_run"))

    batches = make_batches(fetch.output)
    results = process.expand(batch=batches)
    fetch >> wait >> batches
    summarize(results) >> record()
