"""Healthy DAG for the CWO 1.6 / Airflow 3.2.1 git-sync test.

This file should parse and show up in the UI. It does not schedule itself.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@dag(schedule=None, catchup=False, tags=["cwo-test"])
def hello_cwo():
    @task
    def say_hello():
        print("hello from cwo-dag-tests")

    say_hello()


hello_cwo()
