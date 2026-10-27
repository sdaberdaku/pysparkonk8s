# pysparkonk8s

pysparkonk8s is an Airflow provider that runs `@task.pyspark` tasks as client-mode Spark drivers on Kubernetes. The Airflow worker pod that executes the task becomes the Spark driver, and Spark creates the executors as pods in the same namespace. When the task ends, the session stops and the executors are deleted, so every task gets its own short-lived Spark cluster and nothing stays running between tasks.

The provider renders the driver pod, the executor pod template and the Spark configuration from three small Python objects: `SparkBaseConf`, `SparkDriverConf` and `SparkExecutorConf`. Memory, placement, volumes, tolerations and the rest of the pod shape are set in Python next to the DAG.

It supports Airflow 2.11 and 3.3 with the KubernetesExecutor, Spark 4 and Python 3.10 to 3.14.

## Two entry points

Use the helper when you want to keep the stock `@task.pyspark` decorator:

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

Use the decorator `@task.pyspark_on_k8s` when you prefer one call:

```python
from airflow.sdk import task  # airflow.decorators on Airflow 2
from pysparkonk8s import SparkDriverConf, SparkExecutorConf

driver_conf = SparkDriverConf(image="ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.12")
executor_conf = SparkExecutorConf(image="ghcr.io/sdaberdaku/spark:4.2.0-python3.12")


@task.pyspark_on_k8s(driver_conf=driver_conf, executor_conf=executor_conf)
def count(spark=None):
    return spark.range(1000).count()


count()
```

The class `PySparkOnK8sOperator` is the operator behind the decorator and can be used directly. It is imported lazily, so `import pysparkonk8s` does not import Airflow.

## Where to go next

- [Quick start](quickstart.md) installs the RBAC chart and runs a first DAG.
- [Configuration](configuration.md) lists every field of the three conf objects.
- [Examples](examples.md) covers scaling, placement, volumes, mapped tasks and more.
- [How it works](how-it-works.md) explains the driver pod, the executor template and the lifecycle.
- [Operations](operations.md) covers RBAC, networking, images and troubleshooting.
- [Limitations](limitations.md) lists what is not supported.
