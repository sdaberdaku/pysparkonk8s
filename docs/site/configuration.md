# Configuration

Three dataclasses describe a deployment. All fields are keyword-only.

- `SparkBaseConf` holds cluster-level settings.
- `SparkDriverConf` describes the Airflow worker pod that hosts the driver.
- `SparkExecutorConf` describes the executor pods.

`pyspark_on_k8s(...)`, `@task.pyspark_on_k8s(...)` and `PySparkOnK8sOperator` take them as `base_conf`, `driver_conf` and `executor_conf`. `base_conf` is optional and defaults to `SparkBaseConf()`. They also take `extra`, a mapping of Spark conf keys applied last.

CPU and memory values use `CPU.cores(2)`, `CPU.milli_cores(250)`, `Memory.gibibytes(4)`, `Memory.mebibytes(512)`, `Memory.gigabytes(...)` and `Memory.megabytes(...)`.

## SparkBaseConf

| Field | Default | Spark key(s) | Pod field |
|---|---|---|---|
| `master` | `k8s://https://kubernetes.default.svc.cluster.local:443` | `spark.master` (and `spark.submit.deployMode=client`) | |
| `namespace` | `None` | `spark.kubernetes.namespace`; when `None` it is `${env:SPARK_DRIVER_NAMESPACE}` | |
| `image_pull_policy` | `IfNotPresent` | `spark.kubernetes.container.image.pullPolicy` | |
| `image_pull_secrets` | `None` | `spark.kubernetes.container.image.pullSecrets` (only when set) | |
| `driver_port` | `7078` | `spark.driver.port` | |
| `block_manager_port` | `7079` | `spark.blockManager.port` | |
| `delete_executors_on_termination` | `True` | `spark.kubernetes.executor.deleteOnTermination` | |
| `executor_pod_template_path` | `/tmp/pysparkonk8s/executor-pod-template.yaml` | `spark.kubernetes.executor.podTemplateFile` | |
| `ca_cert_file` | `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt` | `spark.kubernetes.authenticate.submission.caCertFile` | |
| `oauth_token_file` | `/var/run/secrets/kubernetes.io/serviceaccount/token` | `spark.kubernetes.authenticate.submission.oauthTokenFile` | |

The base conf also always renders `spark.kubernetes.driver.pod.name=${env:SPARK_DRIVER_POD_NAME}` and `spark.driver.host=${env:SPARK_LOCAL_IP}`. Spark resolves the `${env:...}` references from the driver pod's environment, which the driver `pod_override` fills from the Downward API (see [How it works](how-it-works.md)).

## SparkDriverConf and SparkExecutorConf

Both classes share the fields below. `image` is required. The Spark keys use `spark.driver.` for the driver and `spark.executor.` for executors.

| Field | Default | Spark key(s) | Pod field |
|---|---|---|---|
| `image` | required | `spark.kubernetes.executor.container.image` (executor) | container `image` |
| `cores` | `CPU.cores(1)` | `spark.{driver,executor}.cores` | cpu request, unless `request_cpu` is set |
| `memory` | `Memory.gibibytes(1)` | `spark.{driver,executor}.memory` | memory request (heap + overhead) |
| `memory_overhead` | `None` | `spark.{driver,executor}.memoryOverhead` | part of the memory request |
| `memory_overhead_factor` | `None` (0.4 driver, 0.1 executor) | used to compute `memoryOverhead` | part of the memory request |
| `request_cpu` | `None` (uses `cores`) | `spark.kubernetes.executor.request.cores` (executor) | cpu request |
| `limit_cpu` | `None` | `spark.kubernetes.executor.limit.cores` (executor) | cpu limit |
| `request_memory` | `None` | | memory request, replaces the computed one (driver only) |
| `limit_memory` | `None` | | memory limit (driver only) |
| `request_ephemeral_storage` | `None` | | ephemeral-storage request |
| `limit_ephemeral_storage` | `None` | | ephemeral-storage limit |
| `extra_java_options` | `-XX:+ExitOnOutOfMemoryError` | `spark.{driver,executor}.extraJavaOptions` | |
| `extra_class_path` | `None` | `spark.{driver,executor}.extraClassPath` | |
| `extra_library_path` | `None` | `spark.{driver,executor}.extraLibraryPath` | |
| `service_account_name` | `None` | `spark.kubernetes.authenticate.executor.serviceAccountName` (executor) | `serviceAccountName` |
| `tolerations` | `None` | | `tolerations` |
| `node_selector` | `None` | | `nodeSelector` |
| `affinity` | `None` for the driver, `FOLLOW_DRIVER` for executors | | `affinity` |
| `volumes` | `None` | | `volumes` |
| `volume_mounts` | `None` | | container `volumeMounts` |
| `env` | `None` | | container `env` |
| `pod_labels` | `{}` | | `metadata.labels` |
| `pod_annotations` | `{}` | | `metadata.annotations` |

Setting `cores`, `memory`, `extra_java_options`, `extra_class_path` or `extra_library_path` to `None` leaves the matching Spark key out. Setting `memory` to `None` also drops `memoryOverhead` and the computed memory request.

The driver conf has no further fields. The driver container is named `base`, as the KubernetesExecutor expects, and its environment always gets the three Downward API variables after the entries of `env`.

### Additional executor fields

| Field | Default | Spark key(s) |
|---|---|---|
| `dynamic_allocation` | `True` | `spark.dynamicAllocation.enabled` |
| `min_executors` | `0` | `spark.dynamicAllocation.minExecutors` (dynamic allocation only) |
| `max_executors` | `10` | `spark.dynamicAllocation.maxExecutors` (dynamic allocation only) |
| `shuffle_tracking` | `True` | `spark.dynamicAllocation.shuffleTracking.enabled` (dynamic allocation only) |
| `instances` | `None` | `spark.executor.instances` (only when dynamic allocation is off) |
| `allocation_batch_size` | `None` | `spark.kubernetes.allocation.batch.size` |
| `pyspark_memory` | `None` | `spark.executor.pyspark.memory` |

The executor container is named `spark-kubernetes-executor`, and Spark is told this with `spark.kubernetes.executor.podTemplateContainerName`.

## Memory sizing

The memory request of a pod is the JVM heap plus the overhead.

- The heap is `memory`.
- The overhead is `memory_overhead` when set, otherwise `max(memory * factor, 384Mi)`. The factor is `memory_overhead_factor` when set, otherwise 0.4 for the driver and 0.1 for executors. The driver default is higher because the driver pod also hosts the Airflow task runner and the Python process.
- For executors, `pyspark_memory` is added to the request. Spark gives the Python workers that amount through `spark.executor.pyspark.memory`.
- `request_memory` replaces the computed request entirely, for the driver pod only.

The memory limit defaults to the request. CPU and ephemeral-storage limits default to none. Every limit that is set is raised to its request if it is lower, so a pod never has a limit below its request. For executor pods, `request_memory` and `limit_memory` have no effect: Spark overwrites both the memory request and the memory limit of the executor container with heap + overhead + PySpark memory, whatever the template says. Control executor memory with `memory`, `memory_overhead` or `memory_overhead_factor`, and `pyspark_memory`.

For example, an executor with `memory=Memory.gibibytes(4)` and `pyspark_memory=Memory.gibibytes(1)` gets an overhead of about 410Mi (0.1 of 4Gi, above the 384Mi floor) and requests about 5.4Gi.

## extra and unsetting keys

`extra` is a mapping from Spark conf keys to values. It is applied after the base, driver and executor conf are merged, so it wins on any key clash. A value of `None` removes the key, which is how you drop a default:

```python
extra = {
    "spark.sql.shuffle.partitions": "64",
    "spark.kubernetes.executor.deleteOnTermination": None,
}
```

## Jinja templating

The Spark conf is passed to the operator as `config_kwargs`, which Airflow renders as a template field. Jinja therefore works in any value that ends up in the Spark conf, including values in `extra`. Pod fields (images, resources, volumes, labels and so on) are rendered once from the conf objects and are static: they are not templated.
