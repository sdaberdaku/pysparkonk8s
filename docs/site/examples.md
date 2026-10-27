# Examples

The snippets assume these imports and image names. `driver_conf` and `executor_conf` are the objects from the [quick start](quickstart.md):

```python
from airflow.sdk import task  # airflow.decorators on Airflow 2
from kubernetes.client import models as k8s
from pysparkonk8s import (
    CPU, Memory, SparkBaseConf, SparkDriverConf, SparkExecutorConf,
    one_per_node, pyspark_on_k8s, same_zone_as,
)

DRIVER_IMAGE = "ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.12"
EXECUTOR_IMAGE = "ghcr.io/sdaberdaku/spark:4.2.0-python3.12"
```

## Dynamic allocation

Dynamic allocation is on by default. Spark scales between `min_executors` and `max_executors` and keeps executors that hold shuffle data (shuffle tracking).

```python
executor_conf = SparkExecutorConf(image=EXECUTOR_IMAGE, min_executors=0, max_executors=20)
```

## Fixed number of executors

```python
executor_conf = SparkExecutorConf(image=EXECUTOR_IMAGE, dynamic_allocation=False, instances=4)
```

## Keep executors in one zone

By default executors prefer the driver's node, then the nodes of the other executors of the run, then the driver's zone. To require a zone, set a placement rule. A rule replaces the default preference.

```python
executor_conf = SparkExecutorConf(image=EXECUTOR_IMAGE, affinity=same_zone_as("eu-west-1a"))
```

## One executor per node

`one_per_node()` keeps the Spark pods of one run on separate nodes. `one_per_node(per_run=False)` does the same across all runs, so no node hosts two Spark pods at all. Rules combine with `&`:

```python
per_run = SparkExecutorConf(image=EXECUTOR_IMAGE, affinity=one_per_node())
cluster_wide = SparkExecutorConf(image=EXECUTOR_IMAGE, affinity=one_per_node(per_run=False))
zonal = SparkExecutorConf(image=EXECUTOR_IMAGE, affinity=same_zone_as("eu-west-1a") & one_per_node())
```

A driver accepts a zone rule and a cluster-wide rule, but not a per-run rule, because it has no run labels to match yet:

```python
driver_conf = SparkDriverConf(image=DRIVER_IMAGE, affinity=same_zone_as("eu-west-1a") & one_per_node(per_run=False))
```

For anything else, pass a Kubernetes `V1Affinity` object as `affinity`.

## Tolerations and node selector

```python
executor_conf = SparkExecutorConf(
    image=EXECUTOR_IMAGE,
    node_selector={"workload": "spark"},
    tolerations=[k8s.V1Toleration(key="spark", operator="Exists", effect="NoSchedule")],
)
```

## Volumes

```python
executor_conf = SparkExecutorConf(
    image=EXECUTOR_IMAGE,
    volumes=[k8s.V1Volume(name="scratch", empty_dir=k8s.V1EmptyDirVolumeSource())],
    volume_mounts=[k8s.V1VolumeMount(name="scratch", mount_path="/scratch")],
    request_ephemeral_storage=Memory.gibibytes(10),
)
```

## Environment variables

```python
driver_conf = SparkDriverConf(image=DRIVER_IMAGE, env=[k8s.V1EnvVar(name="TZ", value="UTC")])
```

## Jinja in the Spark conf

Values in `extra` are templated by Airflow at run time:

```python
overrides = pyspark_on_k8s(
    driver_conf=driver_conf,
    executor_conf=executor_conf,
    extra={"spark.app.name": "{{ dag.dag_id }}-{{ run_id }}"},
)
```

Pod fields cannot use Jinja.

## Mapped tasks

Each mapped task instance gets its own driver pod and its own executors. The decorator form works with `.expand`:

```python
@task.pyspark_on_k8s(driver_conf=driver_conf, executor_conf=executor_conf, max_active_tis_per_dag=2)
def scaled_sum(n: int, spark=None):
    return n * spark.range(1000).selectExpr("sum(id)").collect()[0][0]


scaled_sum.expand(n=[1, 2, 3])
```

The `map_index` label keeps the executors of one mapped instance attached to its own driver.

## Several executors in one deployment

When the default Airflow executor is not the KubernetesExecutor, for example with a multi-executor setup, name it. Spark tasks then run on Kubernetes while the rest of the DAG uses the default:

```python
overrides = pyspark_on_k8s(
    driver_conf=driver_conf,
    executor_conf=executor_conf,
    executor_name="KubernetesExecutor",
)
```

With the decorator, pass `executor="KubernetesExecutor"` to `@task.pyspark_on_k8s`.

## The same task on Spark Connect and on demand

The task body is the same; only the decorator arguments differ. A task using a Spark Connect connection from the stock provider:

```python
@task.pyspark(conn_id="spark_connect")
def report(spark=None):
    return spark.range(10).count()
```

and the on-demand version:

```python
@task.pyspark
def report(spark=None):
    return spark.range(10).count()


report.override(**pyspark_on_k8s(driver_conf=driver_conf, executor_conf=executor_conf))()
```
