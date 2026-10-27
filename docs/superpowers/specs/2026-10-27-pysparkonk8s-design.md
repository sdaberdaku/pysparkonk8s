# pysparkonk8s: on-demand Spark clusters for `@task.pyspark` on Kubernetes

- Status: implemented; this text describes the behaviour of 1.0.0
- Date: 2026-10-27
- Repo: `sdaberdaku/pysparkonk8s` (new, public). PyPI: `airflow-provider-pysparkonk8s`. Import: `pysparkonk8s`.
- Replaces: `sdaberdaku/apache-airflow-providers-pysparkonk8s` (Jan 2024, Airflow 2.8, never released). That repo gets archived with a pointer once 1.0.0 is out.

## Intent

An Airflow task that runs PySpark code on its own ephemeral Spark cluster: the Airflow worker pod is the Spark
driver (client mode), executors are Kubernetes pods created by Spark and deleted when the task ends. The user
writes a normal `@task.pyspark` function and gets a `spark` session; sizing, placement and RBAC details are
declared with small config objects instead of raw Spark conf keys and raw pod specs.

Why now: the 2024 provider is stale (Airflow 2.8, pyspark 3.5, dependency pins that no longer resolve, no
release). Airflow 3 moved the decorator machinery into the Task SDK and the upstream `apache-spark` provider
now ships a `PySparkOperator` plus `@task.pyspark` that already handles Airflow 2.11 and 3.x. The new project
builds on that instead of re-implementing it.

Spark Connect against a long-lived shared cluster stays the ordinary path for most tasks (the stock decorator
does that already). This project is the escape hatch for the task that needs isolation or non-default sizing.

## Goals

1. Two entry points on one core:
   - `pyspark_on_k8s(...)` helper: returns the kwargs for `.override(**...)` on a stock `@task.pyspark` task.
   - `@task.pyspark_on_k8s(...)` decorator and `PySparkOnK8sOperator`: subclasses of the upstream
     `_PySparkDecoratedOperator` / `PySparkOperator`, registered through the provider entrypoint.
2. Airflow 3.x first, Airflow 2.11 also supported (both in CI). Older Airflow out of scope.
3. Spark 4.x (`pyspark>=4.0`). Spark 3.5 out of scope.
4. Config objects (`SparkBaseConf`, `SparkDriverConf`, `SparkExecutorConf`) with `CPU` / `Memory` value types that
   render both the Spark conf and the Kubernetes pod spec of their component.
5. Per-run follow-driver executor placement that is correct for concurrent runs and mapped tasks.
6. Published on PyPI by a tag-triggered workflow, documented with mkdocs-material on GitHub Pages.
7. Ready images on GHCR, built locally by the e2e test on every pull request (nothing is pushed), checked by a
   release pre-flight (every row on a native amd64 and arm64 runner, import and `java -version` smoke test, before
   anything reaches PyPI) and pushed for amd64 and arm64 on release:
   - driver `ghcr.io/sdaberdaku/pysparkonk8s/driver:<provider>-airflow<airflow>-python<python>` (plus `<provider>`
     and `latest` for the default row): the stock Airflow image with a JRE, pyspark and this package. Rows in
     1.0.0: Airflow 3.3.2 on Python 3.10, 3.12 and 3.14.
   Executor images are not built here: the docs point at `ghcr.io/sdaberdaku/spark:<spark>-python<python>` from
   the separate `sdaberdaku/spark-images` project (stock Spark plus a uv-managed Python, pandas and pyarrow) for
   Python 3.11+, and at the stock `spark:4.2.0-python3` for Python 3.10.

## Non-goals

- Local and Spark Connect modes: the stock `@task.pyspark` covers them (`conn_id`, `local[*]`).
- Cluster deploy mode (`spark-submit`-style driver pod): that is the `SparkKubernetesOperator` / Spark Operator job.
- Executor images: provided by `sdaberdaku/spark-images`; users add their jars on top.
- Any catalog, S3, Iceberg, Delta or cloud-provider tuning. Users pass those through `extra`.
- Sizing presets. Users size explicitly.
- Overriding the stock `@task.pyspark` name. ProvidersManager keeps the first decorator registered under a name
  and ignores later ones with a warning; discovery order is not controllable.

## Anti-goals (would make it a failure even if it works)

- Anything that names a specific organisation's clusters, images, buckets, node pools or account IDs. The code
  is written fresh for this repo; the 2024 public repo (same author, Apache 2.0) may be reused. No code is
  copied from private codebases.
- Test-mode branches in production code (the 2024 `is_test_run()` hack that skipped `spark.stop()` under pytest).
- A pod template that needs an initContainer or placeholder tokens resolved at pod start.

## Constraints

- Python 3.10 to 3.14. Airflow 2.11 runs on 3.10 to 3.12, Airflow 3.3 on 3.10 to 3.14; Airflow excludes 3.15,
  and Airflow 3.4 (beta) and the providers released since October 2026 require 3.11+, so 3.10 support ends with
  Airflow 3.3. PySpark requires the same Python minor on driver and executors; the stock `spark:4.2.0-python3`
  images ship Python 3.10; for newer Pythons the docs point at `ghcr.io/sdaberdaku/spark` (documented).
- Dependencies: `apache-airflow>=2.11`, `apache-airflow-providers-apache-spark>=6.3`,
  `apache-airflow-providers-cncf-kubernetes>=10`, `pyspark>=4.0`, `pyyaml`. No pins tighter than lower bounds.
- Own namespace `pysparkonk8s` (Airflow asks third-party providers to stay out of `airflow.providers.*`).
- Tooling: `uv`, `hatchling`, `ruff` (lint + format), `pytest`. `src/` layout.
- The driver image is the Airflow worker image plus a JRE (17 or 21), full `pyspark`, and the jars the job needs;
  the published GHCR image is one such image. The executor image is a stock `spark:4.x` image (or a derived one).
  The config objects have no default image and fail fast if none is set; the docs recommend the GHCR image.
- License Apache 2.0.

## Design

### Core: config objects

```python
from pysparkonk8s import CPU, Memory, SparkBaseConf, SparkDriverConf, SparkExecutorConf
```

`SparkBaseConf` (cluster-level):
`master` (default `k8s://https://kubernetes.default.svc:443`), `namespace` (default: the pod's
own, via Downward API env; when set it must equal the worker pod's namespace), `image_pull_policy`,
`image_pull_secrets` (executor pods only; the driver is the worker pod), `driver_port` (7078),
`block_manager_port` (7079), `delete_executors_on_termination` (true), `executor_pod_template_path`
(default `/tmp/pysparkonk8s/executor-pod-template.yaml`), `ca_cert_file`, `oauth_token_file` (default `None`;
when set they render the client-mode keys `spark.kubernetes.authenticate.caCertFile` and
`spark.kubernetes.authenticate.oauthTokenFile`; unset, Spark's client uses and refreshes the mounted service
account token).

`_K8sConf` (shared by driver and executor; abstract):
`image` (required), `cores`, `memory`, `memory_overhead`, `memory_overhead_factor`, `request_cpu`, `limit_cpu`,
`request_memory`, `limit_memory`, `request_ephemeral_storage`, `limit_ephemeral_storage`, `extra_java_options`,
`extra_class_path`, `extra_library_path`, `service_account_name`, `tolerations`, `node_selector`, `affinity`,
`volumes`, `volume_mounts`, `env`, `pod_labels`, `pod_annotations`. Every optional field that is `None` is
omitted from the render. `cores` must be a whole number of cores (Spark reads `spark.*.cores` as an integer);
a fractional pod CPU goes in `request_cpu`. The `pysparkonk8s/component` label is applied after `pod_labels`, so
users cannot override it. `CPU(milli=...)` and `Memory(bytes=...)` are keyword-only. Requests default to Spark's own values (cpu = cores, memory = heap + overhead);
limits are clamped up to the request, and `spark.kubernetes.executor.limit.cores` renders the clamped value. Kubernetes types are the `kubernetes.client` models, imported lazily so
DAG parsing stays cheap.

`SparkDriverConf`: container name fixed to `base` (KubernetesExecutor injects the task there). Renders the
`pod_override` V1Pod: resources, the component label `pysparkonk8s/component=driver`, Downward API env vars
`SPARK_DRIVER_POD_NAME`, `SPARK_LOCAL_IP`, `SPARK_DRIVER_NAMESPACE`, plus the user's extras. No affinity by
default (a driver never follows anything; `affinity=FOLLOW_DRIVER` on the driver raises `ValueError`). Memory request defaults to heap + overhead with the driver overhead
factor defaulting to 0.4 (Spark's rule for non-JVM drivers: the pod also hosts the Airflow task runner, Python
and py4j), floor 384Mi. Executors use Spark's 0.1 default. Both are overridable with `memory_overhead` or
`memory_overhead_factor`.

`SparkExecutorConf`: adds `instances`, `dynamic_allocation` (bool, default true), `min_executors`, `max_executors`,
`allocation_batch_size`, `shuffle_tracking`, `pyspark_memory` (`spark.executor.pyspark.memory`, added to the
pod memory request like Spark does). Container name `spark-kubernetes-executor` (matches Spark's
`podTemplateContainerName`). Default `affinity` is the follow-driver preferred podAffinity (below). Renders the
executor pod template V1Pod and the `spark.kubernetes.executor.*` conf keys. Executor memory limit is not
a field: Spark overwrites the template's memory request and limit with heap + overhead.

`render_spark_conf()` on each object returns a flat `dict[str, str]`; `render_pod_specification(run)` on the
two k8s objects returns a `V1Pod`, where `run` carries the run-identity labels (see placement).

`build_spark_conf(base, driver, executor, extra=None)` merges base, driver, executor, then `extra` (a value of
`None` unsets the key). This is what lands in `config_kwargs`, at DAG-parse time for the helper and in
`__init__` for the operator.

Runtime-only values never enter the conf as literals. The driver pod name, pod IP and namespace are only known
inside the pod, so the conf carries Spark's own substitution syntax, `spark.kubernetes.driver.pod.name=
${env:SPARK_DRIVER_POD_NAME}`, `spark.driver.host=${env:SPARK_LOCAL_IP}`, `spark.kubernetes.namespace=
${env:SPARK_DRIVER_NAMESPACE}` (unless `namespace` is set), and the `pod_override` defines those env vars from
the Downward API. Spark resolves them when the session starts. The executor template path is a plain string
(default `/tmp/pysparkonk8s/executor-pod-template.yaml`, one task per pod under KubernetesExecutor). If the
env vars are missing at runtime (task not launched through the `pod_override`, for example on a Celery
worker), the template writer raises a clear error before the session starts.

Templating: the conf objects are not `template_fields`; `config_kwargs` is. Jinja works in every value that ends
up in the Spark conf (object fields that render to conf strings, and `extra`) on all three entry points. The
operator inherits `config_kwargs` from `PySparkOperator`. The decorated operator lists only `op_args` and
`op_kwargs` first in its MRO, so `_PySparkOnK8sDecoratedOperator.template_fields` is the union of upstream's
decorator fields and the operator's. The stock `@task.pyspark` does not template `config_kwargs`, so the
helper's `pre_execute` hook renders it with `task.render_template` and the task context (Airflow renders
template fields before `pre_execute` on 2.11 and 3.x, and rendering a rendered value again changes nothing).
Pod spec fields (images, labels, affinity, volumes) are static: the driver `pod_override` is consumed by the
scheduler before any task runs. Documented as such.

### Core: writing the executor template at runtime

Spark reads the executor pod template from a file, and the file must exist before `SparkSession.getOrCreate()`.
Both entry points write it inside the driver pod right before the session starts, using the real
`dag_id`, `task_id`, `run_id` and `map_index` from the task context, passed through
`make_safe_label_value` from the cncf provider so they equal the labels KubernetesExecutor stamped on the
worker pod.

- Helper: a `pre_execute` hook (still a `BaseOperator` parameter in the Airflow 3 Task SDK; the task runner
  calls it right before `execute`).
- Decorator/operator: an `execute()` override that writes the file, then calls the upstream implementation,
  which builds the session from `config_kwargs` and stops it in `finally`.

### Mapped tasks

The scheduler never runs the operator's `__init__` for mapped tasks, so the decorator renders the driver
`pod_override` when it is applied and stores it in the decorator kwargs (`partial_kwargs`). On the worker the
operator keeps that incoming pod (it carries our component label; any other `pod_override` raises), and at the
start of `execute()` a mapped task (`map_index >= 0`) compares it with a fresh render of its own `driver_conf`.
A mismatch, which `.override(driver_conf=...)` causes because `override` does not re-run the decorator, raises
`PySparkOnK8sError` telling the user to pass `driver_conf` to `@task.pyspark_on_k8s(...)` itself. The classic
`PySparkOnK8sOperator.partial(...).expand(...)` gets no `pod_override` at all; mapped tasks use the decorator.

### Placement

KubernetesExecutor labels every worker pod with `dag_id`, `task_id`, `run_id`, `try_number` and, for mapped
tasks, `map_index`. The executor template carries the same labels plus `pysparkonk8s/component=executor`; the
driver pod gets `pysparkonk8s/component=driver` through `pod_override`. The default executor affinity is a
preferred podAffinity: same node as this run's driver (weight 100), same node as this run's executors (75), same
zone as the driver (50), same zone as the executors (25). Concurrent runs of the same task select different
`run_id` values, so they never attract each other's executors.

Helpers: `same_zone_as(zone: str)` returns a required nodeAffinity on `topology.kubernetes.io/zone`;
`one_per_node(per_run=True)` returns a required podAntiAffinity on the component label, scoped to this run's
labels by default (so concurrent jobs do not sit Pending fighting for nodes); `per_run=False` repels every
pysparkonk8s pod on the cluster, for dedicated node pools. The run-scoped selector needs the run labels, so
helper outputs are rendered into the executor template by the runtime writer, the same path the follow-driver
default uses. Users can pass any `V1Affinity`.

### Entry point 1: helper

```python
from airflow.decorators import task
from pysparkonk8s import pyspark_on_k8s, SparkDriverConf, SparkExecutorConf, CPU, Memory

@task.pyspark
def job(spark):
    spark.range(10**6).count()

job.override(**pyspark_on_k8s(
    driver_conf=SparkDriverConf(image="ghcr.io/me/airflow-spark:3.1.0", cores=CPU.cores(2), memory=Memory.gibibytes(4)),
    executor_conf=SparkExecutorConf(image="spark:4.1.0-python3", cores=CPU.cores(4), memory=Memory.gibibytes(8), max_executors=10),
    extra={"spark.sql.shuffle.partitions": "200"},
    executor_name="KubernetesExecutor",   # optional, for multi-executor deployments
))
```

Returns `{"executor_config": {"pod_override": V1Pod}, "config_kwargs": {...}, "pre_execute": hook}` and
`executor` when `executor_name` is given. `conn_id` is left to the caller (it must be unset for this mode; the
helper sets `conn_id=None` explicitly to override a decorator default).

### Entry point 2: decorator and operator

```python
@task.pyspark_on_k8s(driver_conf=SparkDriverConf(...), executor_conf=SparkExecutorConf(...))
def job(spark): ...
```

The kwargs are named `driver_conf`, `executor_conf`, `base_conf` on both entry points: `executor` is Airflow's own
per-task argument (the executor name), and `BaseOperator` would overwrite it. `PySparkOnK8sOperator(PySparkOperator)`
takes `base_conf`, `driver_conf`, `executor_conf`, `extra`; sets `executor_config`
in `__init__` (merging with any user-given `executor_config`) and `config_kwargs` from `build_spark_conf`.
`_PySparkOnK8sDecoratedOperator(DecoratedOperator, PySparkOnK8sOperator)` mirrors upstream's decorator class.
Provider entrypoint `apache_airflow_provider` exposes `get_provider_info()` with the task decorator named
`pyspark_on_k8s`. The conf objects are not template fields (see Templating above); `PySparkOnK8sOperator` is exported lazily from the
package so `import pysparkonk8s` stays free of Airflow imports.

### RBAC

Client-mode Spark needs, in the worker namespace: `pods` (create, get, list, watch, delete, patch, deletecollection),
`pods/log` (get, list), `configmaps` (create, get, list, watch, delete, patch) for the executor config map and
`services` (create, get, list, delete) for the driver headless service. Shipped as a tiny Helm
chart `charts/pysparkonk8s-rbac` (Role + RoleBinding, value `serviceAccountName`, default `airflow-worker`),
published as an OCI chart to `ghcr.io/sdaberdaku/charts/pysparkonk8s-rbac` on tag. Also shown as plain YAML in
the docs.

### Package layout

```
src/pysparkonk8s/
  __init__.py        public names
  resources.py       CPU, Memory
  conf.py            SparkBaseConf, SparkDriverConf, SparkExecutorConf, build_spark_conf, affinity helpers
  template.py        write_executor_pod_template(run, executor_conf, path)
  helper.py          pyspark_on_k8s()
  operators.py       PySparkOnK8sOperator
  decorators.py      _PySparkOnK8sDecoratedOperator, pyspark_on_k8s_task
  provider_info.py   get_provider_info(), __version__
charts/pysparkonk8s-rbac/
docs/site/           mkdocs-material
examples/dags/
tests/
```

### Testing

Unit (pytest, no cluster): every conf field renders or is omitted; clamping; `extra` unset; follow-driver
affinity carries the run labels; template file written with safe label values for a mapped and an unmapped
task; helper returns the right keys; decorator is registered and resolvable through `ProvidersManager`;
operator sets `executor_config`. Matrix: Airflow 2.11 on Python 3.10 and 3.12; Airflow 3.3 on 3.10, 3.12 and 3.14.

End to end (kind, Airflow 3 only; 2.11 is covered by the unit matrix), two rows: Python 3.10 driver with the
stock `spark:4.2.0-python3` executor, and Python 3.14 driver with `ghcr.io/sdaberdaku/spark:4.2.0-python3.14`.
Each row builds the driver image, installs Airflow with the official Helm chart and KubernetesExecutor, mount the example DAGs, trigger a
DAG that runs one task through each entry point plus one mapped task, assert they succeed, assert executor pods
appeared and are gone afterwards. Spark 4.x only.

### CI, release, protection

- `ci.yml`: on pull requests (skipped for drafts), on push to `main`, on manual runs and as a reusable workflow:
  ruff, unit matrix, e2e, final `ci-ok` job.
- Renovate (Mend app) keeps Python dependencies, the driver image inputs (`docker/images.json`), the e2e Helm chart
  version and action majors current; patch-level and action updates automerge after `ci-ok`.
- `release.yml`: on tag `v*` or by hand. `test` (full CI), `build` (sdist and wheel, version checks, `dist`
  artifact) and `build-check` (every driver image row on native amd64 and arm64 runners, loaded, not pushed,
  smoke tested) run in both cases, so a manual run is a dry run before tagging. Only for tags: `publish` (PyPI
  trusted publishing with `skip-existing`, idempotent `gh release create`, environment `pypi`), then `image`
  (multi-arch push) and `chart` (OCI chart), which need `publish`. The chart version must equal the tag on every
  release. First push creates private GHCR packages; they are set to public once, after the first release.
- `pages.yml`: mkdocs build and deploy on push to `main` touching `docs/**` or `mkdocs.yml`.
- Protection on `main`: pull request required, `ci-ok` required, linear history, force push only by the owner.
- Version 1.0.0 first release.

### Documentation (mkdocs-material)

index, quickstart (RBAC, image, first DAG), configuration (every field of the three objects, with Spark key and
pod field it maps to), how-it-works (worker pod as driver, template write, labels and placement, lifecycle),
examples (sizing, dynamic allocation, fixed executors, zone pin, one per node, tolerations and node selector,
volumes, Jinja in `extra`, mapped tasks, multi-executor), operations (RBAC, network policy ports, images, logs,
troubleshooting), limitations, development, release, changelog.

## Left to the builder

- Whether the runtime hook injects anything into `self.config_kwargs` at all (the `${env:...}` design should make
  that unnecessary), and the exact shape of the "run" object passed to the template writer.
- Exact names of minor fields and the `CPU` / `Memory` API surface (parsers, arithmetic, `to_spark()` /
  `to_k8s()`), keeping the 2024 repo's shape where it fits.
- How the e2e job gets images into kind and how it polls the DAG run.
- The Dockerfile build arguments (`AIRFLOW_VERSION`, `PYTHON_VERSION`, `PYSPARK_VERSION`) and the image matrix file.
  The image installs `pyspark` and `pyspark-client` at the same version, with `apache-airflow` pinned so pip does
  not move Airflow.
- mkdocs theme details.
