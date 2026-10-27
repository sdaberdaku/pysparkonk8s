# Operations

## RBAC { #rbac }

The chart `pysparkonk8s-rbac` creates a Role and a RoleBinding in the release namespace for the service account named in `serviceAccountName`. The Role grants:

| Resources | Verbs |
|---|---|
| `pods` | create, get, list, watch, delete, patch, deletecollection |
| `pods/log` | get, list |
| `configmaps` | create, get, list, watch, delete, patch |
| `services` | create, get, list, delete |

Spark creates and deletes the executor pods and the configmaps that carry each executor's configuration, which is why the Role covers `configmaps`. Install the chart in the namespace where the worker pods run; use the Helm values `labels` and `annotations` for extra metadata.

```bash
helm install pysparkonk8s-rbac oci://ghcr.io/sdaberdaku/charts/pysparkonk8s-rbac \
  --version 1.0.0 -n airflow --set serviceAccountName=airflow-worker
```

## Networking

Executors connect to the driver, so the driver pod must accept traffic from executor pods on the driver port `7078` and the block manager port `7079`. These are the defaults of `driver_port` and `block_manager_port`. If a NetworkPolicy restricts ingress to the worker pods, allow those two ports from pods labelled `pysparkonk8s/component=executor`. Executors also talk to each other on the block manager port.

## Images

The driver and the executors must satisfy these conditions.

- Both need a JRE. The driver image installs `openjdk-17-jre-headless`, because the Debian bookworm base of the Airflow image has no Java 21.
- The `pyspark` package on the driver must have the same version as the Spark of the executor image. The published images use 4.2.0.
- The Python minor version must be the same on both sides. Patch versions can differ.

The published driver images are built from `docker/driver.Dockerfile`: `apache/airflow`, `openjdk-17-jre-headless`, `pyspark==4.2.0` and the provider wheel. Tags have the form `<provider version>-airflow<airflow version>-python<minor>` for Python 3.10, 3.12 and 3.14.

Executor images come from two places. For Python 3.10 the stock `spark:4.2.0-python3` image works. For Python 3.11 to 3.15 the project `sdaberdaku/spark-images` publishes `ghcr.io/sdaberdaku/spark:4.2.0-python3.<minor>`: a python-slim base with a JRE, stock Spark, pandas and pyarrow. To add jars or Python packages, build an image `FROM` one of these and copy the jars to `/opt/spark/jars`.

## Logs and the Spark UI

The task log shows the driver output. Executor logs are the logs of the executor pods: `kubectl logs <executor pod>`. Because executors are deleted at the end of the session, read them while the task runs, or set `delete_executors_on_termination=False` on `SparkBaseConf` to keep finished executors for debugging. Cleanup of those pods is then up to you.

The provider does not set `spark.ui.enabled`, so Spark's own default applies and the UI listens on port 4040 of the driver pod. Reach it with `kubectl port-forward` to the driver pod, or turn it off with `extra={"spark.ui.enabled": "false"}`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `PySparkOnK8sError: Missing environment variables: ...` before the session starts | The task did not run in the driver `pod_override`. Run it on the KubernetesExecutor, or pass `executor_name="KubernetesExecutor"` to the helper (`executor="KubernetesExecutor"` to the decorator). |
| Executors stay `Pending` | Check the pod events. With a required rule such as `same_zone_as` or `one_per_node`, no node may satisfy it. Relax the rule, or add capacity in that zone. |
| Driver pod `OOMKilled` | The driver pod also runs the Airflow task runner and Python. Raise `memory` or `memory_overhead` on `SparkDriverConf`. |
| Executor pod `OOMKilled` | Raise `memory_overhead`, or `pyspark_memory` when Python workers use the memory. |
| `ImagePullBackOff` | Check the image name and tag and set `image_pull_secrets` on `SparkBaseConf` for a private registry. |
| Executors cannot register with the driver | Check the NetworkPolicy for ports 7078 and 7079. |
| Python version mismatch error at the first action | Use a driver and executor image with the same Python minor. |
| `Forbidden` errors creating pods | The worker service account lacks the Role. Install the RBAC chart in the task namespace. |
