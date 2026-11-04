# cwo-dag-tests

Public DAG repo for Cloudera Workflow Orchestrator 1.6 testing on Airflow 3.2.1.

Point CWO at this repository with branch `main` and an empty Git SubPath. There are no secrets and no symlinks.

| File | Expected result |
| --- | --- |
| `hello_cwo.py` | Parses. DAG id `hello_cwo`. Manual runs only. |
| `broken_parse.py` | Import error for this file only. The DAG processor stays up and `hello_cwo` still loads. |
