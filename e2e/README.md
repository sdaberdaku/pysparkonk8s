# End-to-end test

`run.sh` installs Airflow 3 (official Helm chart, KubernetesExecutor) on a kind cluster and runs the
three DAGs in `examples/dags`. Each DAG starts a client-mode Spark session in the worker pod, with
executors as pods. The script checks that every DagRun succeeds, that executor pods appeared and are
gone afterwards, and that the XCom values are the expected sums.

Needs docker, kind, kubectl, helm, uv, curl and jq, plus the chart repo:

```bash
helm repo add apache-airflow https://airflow.apache.org
bash e2e/run.sh
```

Environment:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PYTHON_VERSION` | `3.10` | Python of the driver image (`apache/airflow:<version>-python<minor>`) |
| `EXECUTOR_IMAGE` | `spark:4.2.0-python3` | Executor image; its Python minor must match the driver |
| `CLUSTER` | `pysparkonk8s` | kind cluster name |
| `KEEP_CLUSTER` | unset | Set to `1` to keep the cluster after the run (deleted otherwise; never deleted in CI) |
| `ARTIFACT_DIR` | `e2e/out` | Pod logs, events and task logs written when a run fails |

For Python 3.14 use the matching executor image:

```bash
PYTHON_VERSION=3.14 EXECUTOR_IMAGE=ghcr.io/sdaberdaku/spark:4.2.0-python3.14 bash e2e/run.sh
```

The driver image is `docker/driver.Dockerfile` (Airflow, OpenJDK 17, PySpark and the wheel built by
`uv build`); the example DAGs are copied into a second image layer on top of it.
