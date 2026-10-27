# How it works

## The worker pod is the driver

The provider runs Spark in client mode. Spark's Kubernetes backend needs to know which pod hosts the driver so it can attach the executors to it. The KubernetesExecutor already starts one pod per task, so the provider makes that pod the driver instead of launching a separate one.

It does this through the task's `executor_config`. The helper, the decorator and the operator render `driver_conf` into a `pod_override` with the driver image, resources, placement and environment. The KubernetesExecutor merges it into the worker pod it creates.

The `pod_override` adds three environment variables from the Downward API:

| Variable | Source | Used for |
|---|---|---|
| `SPARK_DRIVER_POD_NAME` | `metadata.name` | `spark.kubernetes.driver.pod.name` |
| `SPARK_LOCAL_IP` | `status.podIP` | `spark.driver.host` |
| `SPARK_DRIVER_NAMESPACE` | `metadata.namespace` | `spark.kubernetes.namespace` |

The Spark conf refers to them as `${env:SPARK_DRIVER_POD_NAME}`, `${env:SPARK_LOCAL_IP}` and `${env:SPARK_DRIVER_NAMESPACE}`, and Spark resolves them when the session starts. The namespace entry is replaced by a fixed value when `SparkBaseConf.namespace` is set. If a task did not start through the `pod_override`, for example because it ran on a different Airflow executor, the variables are missing and the task fails before the Spark session starts with a `PySparkOnK8sError` that names them.

## The executor pod template

Executors are created from a pod template file, which carries everything Spark properties cannot express: placement, volumes, tolerations, labels and so on. The file is written inside the driver pod, at `executor_pod_template_path` (default `/tmp/pysparkonk8s/executor-pod-template.yaml`), after the task's templates are rendered and before the session starts. With the helper and the decorator, a `pre_execute` hook writes it; the operator writes it at the start of `execute`. Both check the environment variables first.

The template labels every executor with `dag_id`, `task_id`, `run_id` and, for mapped tasks, `map_index`, the same labels the KubernetesExecutor puts on the driver pod, plus `pysparkonk8s/component=executor`. The driver carries `pysparkonk8s/component=driver`. Label values are made label-safe with the same function the KubernetesExecutor uses.

## Placement

The default executor affinity is `FOLLOW_DRIVER`: a preferred pod affinity scoped by the run labels, so only pods of the same task run attract each other. In order of weight, executors prefer

1. the same node as the driver (100),
2. the same node as other executors of the run (75),
3. the same zone as the driver (50),
4. the same zone as other executors of the run (25).

This keeps shuffle traffic on one node or one zone when capacity allows, without making placement a hard requirement. The rules from `same_zone_as` and `one_per_node` are required rules instead, and replace the preference.

## Mapped tasks

For mapped tasks the scheduler never runs the operator's `__init__`, so the operator cannot add the `pod_override`. The decorator therefore renders it when the task is decorated and stores it in the task arguments. When the operator is constructed on the worker, it renders `driver_conf` again and replaces a `pod_override` that carries the `pysparkonk8s/component: driver` label. A `pod_override` set by the user in `executor_config` is rejected.

## Lifecycle

1. The KubernetesExecutor starts the worker pod from the `pod_override`.
2. The pre-execute hook (or the operator) checks the environment and writes the executor template.
3. The `@task.pyspark` machinery starts the Spark session. Spark creates executor pods owned by the driver pod, from the template.
4. The task function runs.
5. When the session stops, Spark deletes the executors (`spark.kubernetes.executor.deleteOnTermination`, true by default). The executor pods are also owned by the driver pod, so Kubernetes removes them if the driver pod disappears.

## Why the stock decorator name is not replaced

`@task.pyspark` is registered by the Apache Spark provider. Airflow's ProvidersManager keeps the first registration of a decorator name and ignores later ones, so a package cannot override that name. This is why the provider offers `.override(**pyspark_on_k8s(...))` on the stock decorator and, separately, `@task.pyspark_on_k8s`.
