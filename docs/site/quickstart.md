# Quick start

## 1. Grant the worker pods the Spark driver permissions

A Spark driver creates and deletes executor pods, so the service account of the Airflow worker pods needs a Role in the namespace where the tasks run. The chart `pysparkonk8s-rbac` creates the Role and a RoleBinding:

```bash
helm install pysparkonk8s-rbac oci://ghcr.io/sdaberdaku/charts/pysparkonk8s-rbac \
  --version 1.0.0 -n airflow --set serviceAccountName=<worker service account>
```

The rules are listed in [Operations](operations.md#rbac).

## 2. Use a worker image that contains Java, PySpark and the provider

The driver runs inside the Airflow worker pod, so the worker image needs a JRE, `pyspark` and this package. Driver images are published for every release:

| Driver image tag | Python | Executor image |
|---|---|---|
| `ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.10` | 3.10 | `spark:4.2.0-python3` |
| `ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.12` | 3.12 | `ghcr.io/sdaberdaku/spark:4.2.0-python3.12` |
| `ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.14` | 3.14 | `ghcr.io/sdaberdaku/spark:4.2.0-python3.14` |

PySpark requires the same Python minor version on the driver and on the executors, so each row pairs a driver with the executor image of the same minor. The images `ghcr.io/sdaberdaku/spark:4.2.0-python3.<minor>` exist for Python 3.11 to 3.15; the stock `spark:4.2.0-python3` image ships Python 3.10.

Set the driver image as the image of your Airflow workers (for the official Airflow chart, `images.airflow` or `defaultAirflowRepository` and `defaultAirflowTag`).

To build an image for another Airflow or Python version, or to add your own dependencies, use `docker/driver.Dockerfile` from the repository. It takes the build arguments `AIRFLOW_VERSION`, `PYTHON_VERSION` and `PYSPARK_VERSION`, and installs the wheel from `dist/`. If you build your own image from another base, install the package with `pip install airflow-provider-pysparkonk8s` together with a JRE, `pyspark==4.2.0` and `pyspark-client==4.2.0` (the same version for both).

## 3. Write a DAG

```python
from datetime import datetime

from airflow.sdk import dag, task  # airflow.decorators on Airflow 2
from pysparkonk8s import CPU, Memory, SparkDriverConf, SparkExecutorConf, pyspark_on_k8s

driver_conf = SparkDriverConf(
    image="ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.12",
    cores=CPU.cores(1),
    memory=Memory.gibibytes(2),
)
executor_conf = SparkExecutorConf(
    image="ghcr.io/sdaberdaku/spark:4.2.0-python3.12",
    cores=CPU.cores(2),
    memory=Memory.gibibytes(4),
    max_executors=4,
)


@dag(start_date=datetime(2025, 1, 1), schedule=None, catchup=False)
def spark_sum():
    @task.pyspark
    def sum_ids(spark=None):
        return spark.range(1000).selectExpr("sum(id)").collect()[0][0]

    sum_ids.override(**pyspark_on_k8s(driver_conf=driver_conf, executor_conf=executor_conf))()


spark_sum()
```

## What to expect

Trigger the DAG. The KubernetesExecutor starts one worker pod for the task, and that pod is the Spark driver. In `kubectl get pods -n airflow -L pysparkonk8s/component` the driver shows `driver` in the label column. A few seconds after the Spark session starts, executor pods whose names end in `-exec-<n>` appear with the label `pysparkonk8s/component=executor`. With dynamic allocation (the default) Spark starts with `min_executors` (0) and scales up to `max_executors` (10) as work arrives.

The task log in the Airflow UI is the driver log. When the task finishes, the executors are deleted and the worker pod completes.
