# PySparkOnK8s

[![PyPI](https://img.shields.io/pypi/v/airflow-provider-pysparkonk8s)](https://pypi.org/project/airflow-provider-pysparkonk8s/)
[![CI](https://github.com/sdaberdaku/pysparkonk8s/actions/workflows/ci.yml/badge.svg)](https://github.com/sdaberdaku/pysparkonk8s/actions/workflows/ci.yml)

An Airflow provider that runs `@task.pyspark` tasks as client-mode Spark drivers on Kubernetes. The Airflow worker pod that executes the task becomes the Spark driver, and Spark creates the executors as pods. When the task ends, the executors are deleted, so every task gets its own short-lived Spark cluster.

It supports Airflow 2.11 and 3.3 with the KubernetesExecutor, Spark 4 and Python 3.10 to 3.14.

## Quick start

Give the worker service account the permissions of a Spark driver:

```bash
helm install pysparkonk8s-rbac oci://ghcr.io/sdaberdaku/charts/pysparkonk8s-rbac \
  --version 1.0.0 -n airflow --set serviceAccountName=<worker service account>
```

Use `ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.12` as the Airflow worker image (it contains Java, PySpark and this package), then write a task:

```python
from airflow.sdk import task  # airflow.decorators on Airflow 2
from pysparkonk8s import SparkDriverConf, SparkExecutorConf, pyspark_on_k8s

driver_conf = SparkDriverConf(image="ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.12")
executor_conf = SparkExecutorConf(image="ghcr.io/sdaberdaku/spark:4.2.0-python3.12")


@task.pyspark
def count(spark=None):
    return spark.range(1000).count()


count.override(**pyspark_on_k8s(driver_conf=driver_conf, executor_conf=executor_conf))()
```

To install the package in your own image, run `pip install airflow-provider-pysparkonk8s`. The image also needs a JRE and `pyspark==4.2.0`.

## Documentation

The full documentation is at https://sdaberdaku.github.io/pysparkonk8s/: configuration reference, examples, how it works, operations and limitations.

## License

Apache-2.0.
