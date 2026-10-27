# Limitations

- Client mode only. There is no cluster mode: the driver is the Airflow worker pod.
- Spark 4 only. The driver image and the executors must run the same Spark version.
- Airflow 2.11 and 3.3. Airflow 3.4 is not supported yet.
- Python 3.10 to 3.14. Airflow does not support 3.15.
- The KubernetesExecutor is required for the driver pod to be built from `driver_conf`.
- Pod fields are not templatable: image, resources, placement, volumes and labels are fixed when the DAG is parsed. Spark conf values, including those in `extra`, can use Jinja.
- One task per driver pod. Each task starts its own Spark session and cluster.
- The stock `@task.pyspark` name cannot be overridden, because Airflow's ProvidersManager keeps the first registration of a decorator name. Use `.override(**pyspark_on_k8s(...))` or `@task.pyspark_on_k8s`.
- `pod_labels` must not override `pysparkonk8s/component`. The driver and executor labels are what the placement rules select on.
- `conn_id` is not supported on the operator: the driver always runs in the cluster.
- The decorator and operator reject an `executor_config` that sets its own `pod_override`.
