# Limitations

- Client mode only. There is no cluster mode: the driver is the Airflow worker pod.
- Spark 4 only. The driver image and the executors must run the same Spark version.
- Airflow 2.11 and 3.3. Airflow 3.4 is not supported yet.
- Python 3.10 to 3.14. Airflow does not support 3.15.
- The KubernetesExecutor is required for the driver pod to be built from `driver_conf`.
- Pod fields are not templatable: image, resources, placement, volumes and labels are fixed when the DAG is parsed. Spark conf values, including those in `extra`, can use Jinja with the helper, the decorator and the operator.
- One task per driver pod. Each task starts its own Spark session and cluster.
- The stock `@task.pyspark` name cannot be overridden, because Airflow's ProvidersManager keeps the first registration of a decorator name. Use `.override(**pyspark_on_k8s(...))` or `@task.pyspark_on_k8s`.
- `pod_labels` cannot override `pysparkonk8s/component`: the provider sets it last, because the placement rules select on it.
- `cores` must be a whole number of cores. Use `request_cpu` for a fractional pod CPU request.
- For mapped tasks, pass `driver_conf` to `@task.pyspark_on_k8s(...)`. `.override(driver_conf=...)` on a mapped task fails at the start of `execute`, because the driver pod is already running from the original conf. Mapped tasks need the decorator: the classic `PySparkOnK8sOperator.partial(...).expand(...)` gets no `pod_override`.
- The helper's keyword arguments replace any `executor_config` or `pre_execute` already set on the stock decorator.
- `image_pull_secrets` applies to executor pods only. The driver is the worker pod, so its pull secrets come from the worker pod template or the service account.
- `conn_id` is not supported on the operator: the driver always runs in the cluster.
- The decorator and operator reject an `executor_config` that sets its own `pod_override`.
