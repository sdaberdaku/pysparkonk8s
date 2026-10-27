# pysparkonk8s Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An Airflow provider that turns a `@task.pyspark` task into a client-mode Spark driver on Kubernetes with ephemeral executor pods, shipped on PyPI with docs, CI and an e2e test.

**Architecture:** Config objects (`SparkBaseConf`, `SparkDriverConf`, `SparkExecutorConf`) render Spark conf and Kubernetes pod specs. A runtime writer puts the executor pod template on disk inside the driver pod with the run's real labels. Two entry points share that core: a `pyspark_on_k8s()` helper returning `.override()` kwargs (with a `pre_execute` hook) for the stock decorator, and a `@task.pyspark_on_k8s` decorator / `PySparkOnK8sOperator` subclassing the upstream `apache-spark` provider.

**Tech Stack:** Python 3.10+, `uv`, `hatchling`, `ruff`, `pytest`, `apache-airflow>=2.11` (3.x first), `apache-airflow-providers-apache-spark>=6.3`, `apache-airflow-providers-cncf-kubernetes>=10`, `pyspark>=4.0`, `pyyaml`, mkdocs-material, Helm, kind, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-27-pysparkonk8s-design.md`

## Global Constraints

- Repo `sdaberdaku/pysparkonk8s`, PyPI `airflow-provider-pysparkonk8s`, import package `pysparkonk8s`, `src/` layout, version `1.0.0`.
- Python `>=3.10,<3.15`. Airflow 2.11 (Python 3.10 to 3.12) and Airflow 3.3 (3.10 to 3.14) both supported. Airflow excludes 3.15. Providers released since October 2026 need 3.11+, so on 3.10 the resolver picks older provider releases; that is expected.
- PySpark needs the same Python minor on driver and executors. Stock `spark:4.2.0-python3` is Python 3.10 (Ubuntu jammy). The docs state the rule and point 3.11+ users at `ghcr.io/sdaberdaku/spark:<spark>-python<python>` (separate project `sdaberdaku/spark-images`).
- Driver image `ghcr.io/sdaberdaku/pysparkonk8s/driver`, tags `<provider>-airflow<airflow>-python<python>` and, for the row marked `latest`, `<provider>` and `latest`. Built from `docker/driver.Dockerfile` with build args `AIRFLOW_VERSION`, `PYTHON_VERSION`, `PYSPARK_VERSION`.
- `docker/images.json`: `[{"airflow": "3.3.2", "python": "3.10", "pyspark": "4.2.0", "latest": true}, {"airflow": "3.3.2", "python": "3.12", "pyspark": "4.2.0"}, {"airflow": "3.3.2", "python": "3.14", "pyspark": "4.2.0"}]`.
- Dependencies are lower bounds only: `apache-airflow>=2.11`, `apache-airflow-providers-apache-spark>=6.3`, `apache-airflow-providers-cncf-kubernetes>=10`, `pyspark>=4.0`, `pyyaml>=6`.
- No default images: a conf without `image` fails at construction.
- No organisation-specific clusters, images, buckets, node pools or account IDs anywhere. No code copied from private codebases; the 2024 public repo at `/home/sebastian/github-sdaberdaku/apache-airflow-providers-pysparkonk8s` (same author, Apache 2.0) may be ported.
- No test-mode branches in production code.
- `kubernetes.client` models imported lazily inside render functions (DAG parsing stays cheap). Module top level imports only stdlib, `pysparkonk8s` and `airflow.providers.common.compat` names.
- Plain prose in docs and README: no "X, not just Y", no punchlines, no bold lead-ins.
- Commit style: `type(scope): subject`, body only when the why is not obvious.

## Review Focus

1. `run_id` values with characters invalid in labels (`manual__2026-10-27T10:00:00+00:00`) must become the same safe value KubernetesExecutor stamped, or the follow-driver selector matches nothing. Test in Task 7.
2. A task launched without the `pod_override` (Celery/Local executor) must fail before `SparkSession` starts with a message naming the missing env vars, not hang on `spark.kubernetes.driver.pod.name` unset. Test in Task 7.
3. Memory limit below request (user sets `limit_memory` < heap + overhead) must clamp up, never produce an invalid pod. Test in Task 4.
4. `extra={"spark.dynamicAllocation.enabled": None}` must remove the key, and `extra` must win over every rendered key. Test in Task 6.
5. Two concurrent runs of the same task must render executor templates whose selectors differ in `run_id` (and `map_index` for mapped instances). Test in Task 5.

---

### Task 1: Project scaffold and provider info

**Files:**
- Create: `pyproject.toml`, `src/pysparkonk8s/__init__.py`, `src/pysparkonk8s/provider_info.py`, `tests/conftest.py`, `tests/test_provider_info.py`, `README.md` (title + one paragraph + "docs coming" link), `LICENSE` (Apache 2.0, copyright 2026 Sebastian Daberdaku), `CHANGELOG.md` (Keep a Changelog, `## [Unreleased]`), `.gitignore` (Python + `.idea/` + `site/` + `airflow_home/`), `.python-version` (`3.12`)

**Interfaces:**
- Produces: `pysparkonk8s.provider_info.__version__ = "1.0.0"`, `get_provider_info() -> dict[str, Any]` with `package-name: "airflow-provider-pysparkonk8s"`, `name: "PySparkOnK8s"`, `description`, `versions: [__version__]`, `task-decorators: [{"name": "pyspark_on_k8s", "class-name": "pysparkonk8s.decorators.pyspark_on_k8s_task"}]`.
- `pyproject.toml`: `[build-system] hatchling`; `[project] name = "airflow-provider-pysparkonk8s"`, `dynamic = ["version"]` from `src/pysparkonk8s/provider_info.py`; dependencies from Global Constraints; `[project.entry-points.apache_airflow_provider] provider_info = "pysparkonk8s.provider_info:get_provider_info"`; `[project.urls]` Homepage `https://sdaberdaku.github.io/pysparkonk8s/`, Source, Changelog; classifiers `Framework :: Apache Airflow`, `Framework :: Apache Airflow :: Provider`; `[dependency-groups] dev = ["pytest>=8", "ruff>=0.14", "pytest-mock>=3"]`; `[tool.ruff] line-length = 120, target-version = "py310"`, `[tool.ruff.lint] select = ["E","F","I","UP","B"]`; `[tool.pytest.ini_options] testpaths = ["tests"]`, `filterwarnings = ["ignore::DeprecationWarning"]`.
- `tests/conftest.py`: sets `AIRFLOW_HOME` to a tmp dir and `AIRFLOW__CORE__UNIT_TEST_MODE=True` before any airflow import; exposes `task` resolved as `from airflow.sdk import task` with fallback `from airflow.decorators import task`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_provider_info.py
from pysparkonk8s.provider_info import __version__, get_provider_info

def test_provider_info_declares_decorator():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-pysparkonk8s"
    assert info["versions"] == [__version__]
    assert info["task-decorators"] == [
        {"name": "pyspark_on_k8s", "class-name": "pysparkonk8s.decorators.pyspark_on_k8s_task"}
    ]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv sync && uv run pytest tests/test_provider_info.py -v`
Expected: FAIL with `ModuleNotFoundError: pysparkonk8s.provider_info`

- [ ] **Step 3: Create the files listed above**

- [ ] **Step 4: Run test and lint**

Run: `uv run pytest tests/test_provider_info.py -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS, no lint findings

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "chore: scaffold package, provider entrypoint and tooling"
```

---

### Task 2: `CPU` and `Memory` value types

**Files:**
- Create: `src/pysparkonk8s/resources.py`, `tests/test_resources.py`
- Port from: `/home/sebastian/github-sdaberdaku/apache-airflow-providers-pysparkonk8s/airflow/providers/pysparkonk8s/resources/{cpu,memory}.py` (drop `serialize`/`deserialize`, drop the `ABCEnumMeta` metaclass, keep behaviour)

**Interfaces:**
- Produces: `class CPU` (immutable, millicores int): `CPU.cores(x: float)`, `CPU.milli_cores(m: int)`, `CPU.from_k8s_spec("1500m" | "1.5" | 2)`, `to_k8s_spec() -> str` (`"1500m"`, whole cores as `"2"`), `to_spark_spec() -> str` (`"1.5"`; whole cores as `"2"`), `__add__`, `__mul__(float)`, `__eq__`, `__lt__`/`__le__`/`__gt__`/`__ge__`, `__str__`.
- `class Memory` (immutable, bytes int): `Memory.gibibytes(x)`, `Memory.mebibytes(x)`, `Memory.gigabytes(x)`, `Memory.megabytes(x)`, `Memory.from_k8s_spec("512Mi" | "2Gi" | "1G" | "1000000")`, `Memory.from_spark_spec("512m" | "2g" | "1t")`, `to_k8s_spec() -> str` (`Mi`, rounding up), `to_spark_spec() -> str` (`m`, rounding up), `__add__`, `__mul__(float)`, comparisons, `__str__`.
- Both raise `ValueError` on negative values and unparsable strings.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_resources.py
import pytest
from pysparkonk8s.resources import CPU, Memory

@pytest.mark.parametrize("spec,milli", [("1500m", 1500), ("1.5", 1500), ("2", 2000), (3, 3000)])
def test_cpu_from_k8s_spec(spec, milli):
    assert CPU.from_k8s_spec(spec) == CPU.milli_cores(milli)

def test_cpu_renders_both_dialects():
    assert CPU.milli_cores(1500).to_k8s_spec() == "1500m"
    assert CPU.cores(2).to_k8s_spec() == "2"
    assert CPU.milli_cores(1500).to_spark_spec() == "1.5"
    assert CPU.cores(2).to_spark_spec() == "2"

def test_cpu_arithmetic_and_order():
    assert CPU.cores(1) + CPU.milli_cores(500) == CPU.milli_cores(1500)
    assert CPU.cores(2) * 1.5 == CPU.cores(3)
    assert CPU.cores(1) < CPU.cores(2)

@pytest.mark.parametrize("spec,rendered", [("512Mi", "512Mi"), ("2Gi", "2048Mi"), ("1G", "954Mi"), ("1048576", "1Mi")])
def test_memory_from_k8s_spec(spec, rendered):
    assert Memory.from_k8s_spec(spec).to_k8s_spec() == rendered

def test_memory_renders_both_dialects():
    assert Memory.gibibytes(2).to_k8s_spec() == "2048Mi"
    assert Memory.gibibytes(2).to_spark_spec() == "2048m"
    assert Memory.from_spark_spec("1g") == Memory.mebibytes(1024)

def test_memory_arithmetic_rounds_up():
    assert (Memory.mebibytes(1000) * 1.4).to_k8s_spec() == "1400Mi"
    assert (Memory.mebibytes(1) * 0.3).to_k8s_spec() == "1Mi"
    assert Memory.gibibytes(1) + Memory.mebibytes(1) == Memory.mebibytes(1025)

@pytest.mark.parametrize("bad", ["-1", "abc", "1x"])
def test_memory_rejects_garbage(bad):
    with pytest.raises(ValueError):
        Memory.from_k8s_spec(bad)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_resources.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `CPU` and `Memory` in `src/pysparkonk8s/resources.py`** as frozen dataclasses holding `milli: int` and `bytes: int`

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_resources.py -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(resources): CPU and Memory value types"
```

---

### Task 3: `SparkBaseConf`

**Files:**
- Create: `src/pysparkonk8s/conf.py`, `tests/test_conf_base.py`

**Interfaces:**
- Produces: `@dataclass(kw_only=True) class SparkBaseConf` with fields and defaults: `master: str = "k8s://https://kubernetes.default.svc.cluster.local:443"`, `namespace: str | None = None`, `image_pull_policy: str = "IfNotPresent"`, `image_pull_secrets: str | None = None`, `driver_port: int = 7078`, `block_manager_port: int = 7079`, `delete_executors_on_termination: bool = True`, `executor_pod_template_path: str = "/tmp/pysparkonk8s/executor-pod-template.yaml"`, `ca_cert_file: str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"`, `oauth_token_file: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"`.
- `render_spark_conf() -> dict[str, str]` emits: `spark.master`, `spark.submit.deployMode=client`, `spark.kubernetes.authenticate.submission.caCertFile`, `spark.kubernetes.authenticate.submission.oauthTokenFile`, `spark.kubernetes.container.image.pullPolicy`, `spark.kubernetes.container.image.pullSecrets` (only when set), `spark.kubernetes.executor.deleteOnTermination`, `spark.driver.port`, `spark.blockManager.port`, `spark.kubernetes.driver.pod.name="${env:SPARK_DRIVER_POD_NAME}"`, `spark.driver.host="${env:SPARK_LOCAL_IP}"`, `spark.kubernetes.namespace` = `namespace` if set else `"${env:SPARK_DRIVER_NAMESPACE}"`, `spark.kubernetes.executor.podTemplateFile`.
- Module constants: `DRIVER_ENV_POD_NAME = "SPARK_DRIVER_POD_NAME"`, `DRIVER_ENV_LOCAL_IP = "SPARK_LOCAL_IP"`, `DRIVER_ENV_NAMESPACE = "SPARK_DRIVER_NAMESPACE"`, `COMPONENT_LABEL = "pysparkonk8s/component"`, `DRIVER_COMPONENT = "driver"`, `EXECUTOR_COMPONENT = "executor"`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_conf_base.py
from pysparkonk8s.conf import SparkBaseConf

def test_base_defaults_use_env_substitution():
    conf = SparkBaseConf().render_spark_conf()
    assert conf["spark.master"] == "k8s://https://kubernetes.default.svc.cluster.local:443"
    assert conf["spark.submit.deployMode"] == "client"
    assert conf["spark.kubernetes.driver.pod.name"] == "${env:SPARK_DRIVER_POD_NAME}"
    assert conf["spark.driver.host"] == "${env:SPARK_LOCAL_IP}"
    assert conf["spark.kubernetes.namespace"] == "${env:SPARK_DRIVER_NAMESPACE}"
    assert conf["spark.kubernetes.executor.podTemplateFile"] == "/tmp/pysparkonk8s/executor-pod-template.yaml"
    assert conf["spark.driver.port"] == "7078"
    assert "spark.kubernetes.container.image.pullSecrets" not in conf

def test_base_explicit_namespace_and_pull_secrets():
    conf = SparkBaseConf(namespace="spark", image_pull_secrets="regcred").render_spark_conf()
    assert conf["spark.kubernetes.namespace"] == "spark"
    assert conf["spark.kubernetes.container.image.pullSecrets"] == "regcred"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_conf_base.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `SparkBaseConf` and the constants in `src/pysparkonk8s/conf.py`**

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/test_conf_base.py -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(conf): SparkBaseConf with runtime env substitution"
```

---

### Task 4: `_K8sConf` resources and `SparkDriverConf`

**Files:**
- Modify: `src/pysparkonk8s/conf.py`
- Create: `tests/test_conf_driver.py`

**Interfaces:**
- Produces: `@dataclass(kw_only=True) class _K8sConf(ABC)` fields: `image: str` (required; `__post_init__` raises `ValueError("image is required")` when empty), `cores: CPU | None = CPU.cores(1)`, `memory: Memory | None = Memory.gibibytes(1)`, `memory_overhead: Memory | None = None`, `memory_overhead_factor: float | None = None`, `request_cpu: CPU | None = None`, `limit_cpu: CPU | None = None`, `request_memory: Memory | None = None`, `limit_memory: Memory | None = None`, `request_ephemeral_storage: Memory | None = None`, `limit_ephemeral_storage: Memory | None = None`, `extra_java_options: str | None = "-XX:+ExitOnOutOfMemoryError"`, `extra_class_path: str | None = None`, `extra_library_path: str | None = None`, `service_account_name: str | None = None`, `tolerations: list | None = None`, `node_selector: dict[str, str] | None = None`, `affinity: Any | None = None`, `volumes: list | None = None`, `volume_mounts: list | None = None`, `env: list | None = None`, `pod_labels: dict[str, str] = {}`, `pod_annotations: dict[str, str] = {}`.
- Class constants per subclass: `component: ClassVar[str]` (`"driver"` / `"executor"`), `container_name: ClassVar[str]`, `default_overhead_factor: ClassVar[float]` (driver `0.4`, executor `0.1`), `MIN_OVERHEAD = Memory.mebibytes(384)`.
- `effective_memory_overhead() -> Memory | None`: `memory_overhead` if set, else `max(memory * factor, MIN_OVERHEAD)` where factor = `memory_overhead_factor` or the class default; `None` when `memory` is `None`.
- `effective_requests() -> tuple[CPU | None, Memory | None, Memory | None]` and `effective_limits()`: cpu request defaults to `cores`; memory request defaults to `memory + overhead`; every limit is clamped up to its request; ephemeral storage limit defaults to `None`.
- `render_spark_conf() -> dict[str, str]`: `spark.<component>.cores` (via `to_spark_spec()`), `spark.<component>.memory`, `spark.<component>.memoryOverhead` (effective, only when `memory` set), `spark.<component>.extraJavaOptions`, `spark.<component>.extraClassPath`, `spark.<component>.extraLibraryPath` (each only when not `None`).
- `render_pod_specification(run: RunIdentity | None = None) -> k8s.V1Pod` (lazy `from kubernetes.client import models as k8s`): `metadata.labels = {COMPONENT_LABEL: component, **pod_labels}`, `metadata.annotations = pod_annotations or None`, one container named `container_name` with `image`, `env` from `_render_env()`, `resources` from effective requests/limits, `volume_mounts`; spec `service_account_name`, `tolerations`, `node_selector`, `affinity` from `_render_affinity(run)`, `volumes`.
- `class SparkDriverConf(_K8sConf)`: `component="driver"`, `container_name="base"`, `default_overhead_factor=0.4`; `_render_env()` returns the user's `env` plus three Downward API vars `SPARK_DRIVER_POD_NAME` (`metadata.name`), `SPARK_LOCAL_IP` (`status.podIP`), `SPARK_DRIVER_NAMESPACE` (`metadata.namespace`); `_render_affinity(run)` returns `self.affinity` unchanged.
- `RunIdentity` is defined in Task 5; for this task declare `from __future__ import annotations` and a forward reference.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_conf_driver.py
import pytest
from pysparkonk8s.conf import COMPONENT_LABEL, SparkDriverConf
from pysparkonk8s.resources import CPU, Memory

IMG = "example.com/airflow-spark:1"

def test_image_required():
    with pytest.raises(ValueError, match="image"):
        SparkDriverConf(image="")

def test_driver_memory_request_defaults_to_heap_plus_40_percent():
    d = SparkDriverConf(image=IMG, cores=CPU.cores(2), memory=Memory.gibibytes(10))
    pod = d.render_pod_specification()
    res = pod.spec.containers[0].resources
    assert res.requests == {"cpu": "2", "memory": "14336Mi"}
    assert res.limits == {"memory": "14336Mi"}
    conf = d.render_spark_conf()
    assert conf["spark.driver.memory"] == "10240m"
    assert conf["spark.driver.memoryOverhead"] == "4096m"
    assert conf["spark.driver.cores"] == "2"

def test_overhead_floor_384mi():
    d = SparkDriverConf(image=IMG, memory=Memory.mebibytes(512))
    assert d.render_spark_conf()["spark.driver.memoryOverhead"] == "384m"

def test_explicit_overhead_and_cpu_request_win():
    d = SparkDriverConf(image=IMG, cores=CPU.cores(4), memory=Memory.gibibytes(4),
                        memory_overhead=Memory.gibibytes(1), request_cpu=CPU.milli_cores(3500),
                        limit_cpu=CPU.cores(4))
    res = d.render_pod_specification().spec.containers[0].resources
    assert res.requests == {"cpu": "3500m", "memory": "5120Mi"}
    assert res.limits == {"cpu": "4", "memory": "5120Mi"}

def test_limit_below_request_is_clamped_up():
    d = SparkDriverConf(image=IMG, memory=Memory.gibibytes(4), limit_memory=Memory.gibibytes(1),
                        request_cpu=CPU.cores(2), limit_cpu=CPU.cores(1))
    res = d.render_pod_specification().spec.containers[0].resources
    assert res.limits["memory"] == res.requests["memory"]
    assert res.limits["cpu"] == "2"

def test_driver_pod_has_component_label_and_downward_env():
    pod = SparkDriverConf(image=IMG, pod_labels={"team": "data"}).render_pod_specification()
    assert pod.metadata.labels == {COMPONENT_LABEL: "driver", "team": "data"}
    c = pod.spec.containers[0]
    assert c.name == "base"
    by_name = {e.name: e.value_from.field_ref.field_path for e in c.env}
    assert by_name == {"SPARK_DRIVER_POD_NAME": "metadata.name", "SPARK_LOCAL_IP": "status.podIP",
                       "SPARK_DRIVER_NAMESPACE": "metadata.namespace"}

def test_none_fields_are_omitted():
    d = SparkDriverConf(image=IMG, cores=None, memory=None, extra_java_options=None)
    conf = d.render_spark_conf()
    assert conf == {}
    res = d.render_pod_specification().spec.containers[0].resources
    assert res.requests is None and res.limits is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_conf_driver.py -v`
Expected: FAIL with `ImportError: SparkDriverConf`

- [ ] **Step 3: Implement `_K8sConf` and `SparkDriverConf` in `src/pysparkonk8s/conf.py`**

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/ -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(conf): shared pod sizing and SparkDriverConf"
```

---

### Task 5: `SparkExecutorConf`, `RunIdentity` and placement helpers

**Files:**
- Modify: `src/pysparkonk8s/conf.py`
- Create: `tests/test_conf_executor.py`

**Interfaces:**
- Produces: `@dataclass(frozen=True) class RunIdentity`: `dag_id: str`, `task_id: str`, `run_id: str`, `map_index: int | None = None` (values already label-safe); `labels() -> dict[str, str]` returns `{"dag_id", "task_id", "run_id"}` plus `"map_index": str(i)` when not `None`. Label keys are exactly the ones KubernetesExecutor stamps.
- `class SparkExecutorConf(_K8sConf)`: `component="executor"`, `container_name="spark-kubernetes-executor"`, `default_overhead_factor=0.1`; extra fields `instances: int | None = None`, `dynamic_allocation: bool = True`, `min_executors: int = 0`, `max_executors: int = 10`, `allocation_batch_size: int | None = None`, `shuffle_tracking: bool = True`, `pyspark_memory: Memory | None = None`, `affinity: Any | None = FOLLOW_DRIVER` where `FOLLOW_DRIVER` is a module-level sentinel object.
- `render_spark_conf()` adds: `spark.kubernetes.executor.container.image`, `spark.kubernetes.executor.podTemplateContainerName`, `spark.kubernetes.authenticate.executor.serviceAccountName` (when set), `spark.kubernetes.executor.request.cores` (effective cpu request), `spark.kubernetes.executor.limit.cores` (when `limit_cpu` set), `spark.executor.pyspark.memory` (when set), `spark.dynamicAllocation.enabled`, and when enabled `spark.dynamicAllocation.shuffleTracking.enabled`, `spark.dynamicAllocation.minExecutors`, `spark.dynamicAllocation.maxExecutors`; when disabled and `instances` set `spark.executor.instances`; `spark.kubernetes.allocation.batch.size` when set. Memory request in the pod adds `pyspark_memory` to heap + overhead.
- `render_pod_specification(run)`: labels `{COMPONENT_LABEL: "executor", **run.labels(), **pod_labels}` (raises `ValueError` when `run` is `None`); `_render_affinity(run)`: `FOLLOW_DRIVER` → `follow_driver(run)`, `None` → no affinity, a `V1Affinity` → as is, a `PlacementRule` (below) → `rule.render(run)`.
- `def follow_driver(run: RunIdentity) -> k8s.V1Affinity`: preferred podAffinity terms, weights 100/75/50/25, topology `kubernetes.io/hostname` for the first two and `topology.kubernetes.io/zone` for the last two, `match_labels = {**run.labels(), COMPONENT_LABEL: "driver" | "executor"}` alternating driver, executor, driver, executor.
- `@dataclass(frozen=True) class PlacementRule`: `render(run: RunIdentity) -> k8s.V1Affinity`. Two implementations exposed as functions returning rules: `same_zone_as(zone: str) -> PlacementRule` (required nodeAffinity, `topology.kubernetes.io/zone In [zone]`) and `one_per_node(per_run: bool = True) -> PlacementRule` (required podAntiAffinity on `kubernetes.io/hostname`; selector `match_expressions` `COMPONENT_LABEL In [driver, executor]` plus, when `per_run`, `match_labels = run.labels()`). `PlacementRule.__and__` combines two rules into one `V1Affinity` (node affinity from one, pod anti-affinity from the other) so `same_zone_as("eu-west-1a") & one_per_node()` works.
- Driver conf accepts `PlacementRule` too: `_render_affinity(run)` on `_K8sConf` handles the rule case for both subclasses; `SparkDriverConf.render_pod_specification()` is called without a run (parse time), so a per-run rule on the driver raises `ValueError("per-run placement rules apply to executors only")`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_conf_executor.py
import pytest
from pysparkonk8s.conf import (COMPONENT_LABEL, FOLLOW_DRIVER, RunIdentity, SparkDriverConf,
                               SparkExecutorConf, one_per_node, same_zone_as)
from pysparkonk8s.resources import CPU, Memory

IMG = "spark:4.2.0-python3"
RUN = RunIdentity(dag_id="d", task_id="t", run_id="manual-2026", map_index=None)

def test_run_identity_labels():
    assert RUN.labels() == {"dag_id": "d", "task_id": "t", "run_id": "manual-2026"}
    assert RunIdentity("d", "t", "r", 3).labels()["map_index"] == "3"

def test_executor_conf_dynamic_allocation_defaults():
    conf = SparkExecutorConf(image=IMG, cores=CPU.cores(4), memory=Memory.gibibytes(8)).render_spark_conf()
    assert conf["spark.executor.cores"] == "4"
    assert conf["spark.executor.memory"] == "8192m"
    assert conf["spark.executor.memoryOverhead"] == "820m"
    assert conf["spark.kubernetes.executor.request.cores"] == "4"
    assert conf["spark.dynamicAllocation.enabled"] == "true"
    assert conf["spark.dynamicAllocation.shuffleTracking.enabled"] == "true"
    assert conf["spark.dynamicAllocation.minExecutors"] == "0"
    assert conf["spark.dynamicAllocation.maxExecutors"] == "10"
    assert conf["spark.kubernetes.executor.container.image"] == IMG
    assert conf["spark.kubernetes.executor.podTemplateContainerName"] == "spark-kubernetes-executor"
    assert "spark.executor.instances" not in conf

def test_executor_conf_fixed_instances_and_pyspark_memory():
    e = SparkExecutorConf(image=IMG, memory=Memory.gibibytes(1), memory_overhead=Memory.mebibytes(512),
                          pyspark_memory=Memory.mebibytes(512), dynamic_allocation=False, instances=3)
    conf = e.render_spark_conf()
    assert conf["spark.executor.instances"] == "3"
    assert conf["spark.executor.pyspark.memory"] == "512m"
    assert "spark.dynamicAllocation.maxExecutors" not in conf
    assert e.render_pod_specification(RUN).spec.containers[0].resources.requests["memory"] == "2048Mi"

def test_executor_template_carries_run_labels_and_follow_driver_affinity():
    pod = SparkExecutorConf(image=IMG).render_pod_specification(RUN)
    assert pod.metadata.labels == {COMPONENT_LABEL: "executor", **RUN.labels()}
    terms = pod.spec.affinity.pod_affinity.preferred_during_scheduling_ignored_during_execution
    assert [t.weight for t in terms] == [100, 75, 50, 25]
    assert [t.pod_affinity_term.topology_key for t in terms] == [
        "kubernetes.io/hostname", "kubernetes.io/hostname", "topology.kubernetes.io/zone", "topology.kubernetes.io/zone"]
    assert terms[0].pod_affinity_term.label_selector.match_labels == {**RUN.labels(), COMPONENT_LABEL: "driver"}
    assert terms[1].pod_affinity_term.label_selector.match_labels == {**RUN.labels(), COMPONENT_LABEL: "executor"}

def test_concurrent_runs_select_different_drivers():
    e = SparkExecutorConf(image=IMG)
    a = e.render_pod_specification(RunIdentity("d", "t", "run-a"))
    b = e.render_pod_specification(RunIdentity("d", "t", "run-b", map_index=1))
    sel = lambda p: p.spec.affinity.pod_affinity.preferred_during_scheduling_ignored_during_execution[0].pod_affinity_term.label_selector.match_labels
    assert sel(a)["run_id"] == "run-a" and "map_index" not in sel(a)
    assert sel(b)["run_id"] == "run-b" and sel(b)["map_index"] == "1"

def test_executor_requires_run():
    with pytest.raises(ValueError):
        SparkExecutorConf(image=IMG).render_pod_specification(None)

def test_affinity_none_and_explicit():
    assert SparkExecutorConf(image=IMG, affinity=None).render_pod_specification(RUN).spec.affinity is None
    from kubernetes.client import models as k8s
    custom = k8s.V1Affinity()
    assert SparkExecutorConf(image=IMG, affinity=custom).render_pod_specification(RUN).spec.affinity is custom

def test_placement_rules():
    rule = same_zone_as("eu-west-1a") & one_per_node()
    aff = SparkExecutorConf(image=IMG, affinity=rule).render_pod_specification(RUN).spec.affinity
    req = aff.node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms[0].match_expressions[0]
    assert (req.key, req.operator, req.values) == ("topology.kubernetes.io/zone", "In", ["eu-west-1a"])
    anti = aff.pod_anti_affinity.required_during_scheduling_ignored_during_execution[0]
    assert anti.topology_key == "kubernetes.io/hostname"
    assert anti.label_selector.match_labels == RUN.labels()
    assert anti.label_selector.match_expressions[0].values == ["driver", "executor"]

def test_one_per_node_cluster_wide_has_no_run_labels():
    aff = SparkExecutorConf(image=IMG, affinity=one_per_node(per_run=False)).render_pod_specification(RUN).spec.affinity
    assert aff.pod_anti_affinity.required_during_scheduling_ignored_during_execution[0].label_selector.match_labels is None

def test_driver_accepts_zone_rule_but_not_per_run_rule():
    assert SparkDriverConf(image=IMG, affinity=same_zone_as("z")).render_pod_specification().spec.affinity.node_affinity
    with pytest.raises(ValueError, match="executors only"):
        SparkDriverConf(image=IMG, affinity=one_per_node()).render_pod_specification()
    assert SparkDriverConf(image=IMG, affinity=one_per_node(per_run=False)).render_pod_specification().spec.affinity.pod_anti_affinity
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_conf_executor.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `RunIdentity`, `SparkExecutorConf`, `FOLLOW_DRIVER`, `follow_driver`, `PlacementRule`, `same_zone_as`, `one_per_node` in `src/pysparkonk8s/conf.py`**

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/ -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(conf): SparkExecutorConf, run identity labels and placement rules"
```

---

### Task 6: `build_spark_conf`

**Files:**
- Modify: `src/pysparkonk8s/conf.py`
- Create: `tests/test_build_conf.py`

**Interfaces:**
- Produces: `def build_spark_conf(base: SparkBaseConf, driver: SparkDriverConf, executor: SparkExecutorConf, extra: Mapping[str, str | None] | None = None) -> dict[str, str]`. Order: base, driver, executor, extra; an `extra` value of `None` removes the key; all values are `str`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_build_conf.py
from pysparkonk8s.conf import SparkBaseConf, SparkDriverConf, SparkExecutorConf, build_spark_conf

def test_merge_order_and_unset():
    conf = build_spark_conf(SparkBaseConf(), SparkDriverConf(image="d"), SparkExecutorConf(image="e"),
                            extra={"spark.dynamicAllocation.enabled": None, "spark.sql.shuffle.partitions": "200",
                                   "spark.master": "k8s://https://other:6443"})
    assert "spark.dynamicAllocation.enabled" not in conf
    assert conf["spark.sql.shuffle.partitions"] == "200"
    assert conf["spark.master"] == "k8s://https://other:6443"
    assert conf["spark.driver.memory"] == "1024m"
    assert conf["spark.kubernetes.executor.container.image"] == "e"
    assert all(isinstance(v, str) for v in conf.values())
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_build_conf.py -v`
Expected: FAIL with `ImportError: build_spark_conf`

- [ ] **Step 3: Implement `build_spark_conf`**

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/ -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(conf): build_spark_conf merge with extra overrides"
```

---

### Task 7: Runtime executor template writer

**Files:**
- Create: `src/pysparkonk8s/template.py`, `src/pysparkonk8s/errors.py`, `tests/test_template.py`

**Interfaces:**
- Produces: `class PySparkOnK8sError(Exception)` in `errors.py`.
- `RunIdentity.from_context(context: Mapping) -> RunIdentity` (add as `classmethod` on `RunIdentity` in `conf.py`): `dag_id = context["dag"].dag_id`, `task_id = context["task"].task_id`, `run_id = context["run_id"]`, `map_index = context["ti"].map_index` when `>= 0` else `None`; `dag_id`, `task_id`, `run_id` passed through `make_safe_label_value` from `airflow.providers.cncf.kubernetes.pod_generator`.
- `def check_driver_env(base: SparkBaseConf) -> None`: raises `PySparkOnK8sError` listing the missing names among `SPARK_DRIVER_POD_NAME`, `SPARK_LOCAL_IP` and, when `base.namespace is None`, `SPARK_DRIVER_NAMESPACE`, with the sentence "The task is not running in the driver pod_override; use the KubernetesExecutor (or executor=\"KubernetesExecutor\" on the task)."
- `def write_executor_pod_template(executor: SparkExecutorConf, run: RunIdentity, path: str) -> str`: creates parent dirs, `yaml.safe_dump(PodGenerator.serialize_pod(executor.render_pod_specification(run)))`, returns the path.
- `def prepare_driver(base: SparkBaseConf, executor: SparkExecutorConf, context: Mapping) -> str`: `check_driver_env`, `RunIdentity.from_context`, `write_executor_pod_template(..., base.executor_pod_template_path)`. Both entry points call this.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_template.py
import os
import pytest
import yaml
from types import SimpleNamespace
from pysparkonk8s.conf import RunIdentity, SparkBaseConf, SparkExecutorConf
from pysparkonk8s.errors import PySparkOnK8sError
from pysparkonk8s.template import check_driver_env, prepare_driver, write_executor_pod_template

def ctx(run_id="manual__2026-10-27T10:00:00+00:00", map_index=-1):
    return {"dag": SimpleNamespace(dag_id="my dag"), "task": SimpleNamespace(task_id="job"),
            "run_id": run_id, "ti": SimpleNamespace(map_index=map_index)}

def test_run_identity_from_context_uses_safe_label_values():
    from airflow.providers.cncf.kubernetes.pod_generator import make_safe_label_value
    run = RunIdentity.from_context(ctx())
    assert run.run_id == make_safe_label_value("manual__2026-10-27T10:00:00+00:00")
    assert run.dag_id == make_safe_label_value("my dag")
    assert run.map_index is None
    assert RunIdentity.from_context(ctx(map_index=2)).map_index == 2

def test_check_driver_env_lists_missing(monkeypatch):
    for n in ("SPARK_DRIVER_POD_NAME", "SPARK_LOCAL_IP", "SPARK_DRIVER_NAMESPACE"):
        monkeypatch.delenv(n, raising=False)
    with pytest.raises(PySparkOnK8sError, match="SPARK_DRIVER_POD_NAME.*SPARK_LOCAL_IP.*SPARK_DRIVER_NAMESPACE"):
        check_driver_env(SparkBaseConf())
    monkeypatch.setenv("SPARK_DRIVER_POD_NAME", "p"); monkeypatch.setenv("SPARK_LOCAL_IP", "10.0.0.1")
    check_driver_env(SparkBaseConf(namespace="spark"))  # namespace given: env not needed
    with pytest.raises(PySparkOnK8sError, match="SPARK_DRIVER_NAMESPACE"):
        check_driver_env(SparkBaseConf())

def test_write_executor_pod_template(tmp_path):
    path = write_executor_pod_template(SparkExecutorConf(image="e"), RunIdentity("d", "t", "r"), str(tmp_path / "x" / "t.yaml"))
    doc = yaml.safe_load(open(path))
    assert doc["kind"] == "Pod"
    assert doc["metadata"]["labels"]["run_id"] == "r"
    assert doc["spec"]["containers"][0]["name"] == "spark-kubernetes-executor"
    assert "podAffinity" in doc["spec"]["affinity"]

def test_prepare_driver_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_POD_NAME", "p"); monkeypatch.setenv("SPARK_LOCAL_IP", "ip")
    monkeypatch.setenv("SPARK_DRIVER_NAMESPACE", "ns")
    base = SparkBaseConf(executor_pod_template_path=str(tmp_path / "t.yaml"))
    assert prepare_driver(base, SparkExecutorConf(image="e"), ctx()) == str(tmp_path / "t.yaml")
    assert os.path.exists(tmp_path / "t.yaml")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_template.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `errors.py`, `template.py` and `RunIdentity.from_context`**

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/ -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(template): write the executor pod template inside the driver pod"
```

---

### Task 8: `pyspark_on_k8s()` helper for the stock decorator

**Files:**
- Create: `src/pysparkonk8s/helper.py`, `tests/test_helper.py`
- Modify: `src/pysparkonk8s/__init__.py` (export `CPU`, `Memory`, `SparkBaseConf`, `SparkDriverConf`, `SparkExecutorConf`, `FOLLOW_DRIVER`, `same_zone_as`, `one_per_node`, `build_spark_conf`, `pyspark_on_k8s`, `PySparkOnK8sError`, `__version__`)

**Interfaces:**
- Produces: `def pyspark_on_k8s(*, driver: SparkDriverConf, executor: SparkExecutorConf, base: SparkBaseConf | None = None, extra: Mapping[str, str | None] | None = None, executor_name: str | None = None) -> dict[str, Any]` returning `{"conn_id": None, "config_kwargs": build_spark_conf(...), "executor_config": {"pod_override": driver.render_pod_specification()}, "pre_execute": hook}` plus `"executor": executor_name` when given. `hook(context)` calls `prepare_driver(base, executor, context)`. The hook is a module-level class instance (`_PrepareDriver(base, executor)` with `__call__`) so it is picklable.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_helper.py
import pytest
from pysparkonk8s import SparkDriverConf, SparkExecutorConf, pyspark_on_k8s

def test_helper_returns_override_kwargs():
    kw = pyspark_on_k8s(driver=SparkDriverConf(image="d"), executor=SparkExecutorConf(image="e"),
                        extra={"spark.x": "1"}, executor_name="KubernetesExecutor")
    assert kw["conn_id"] is None
    assert kw["executor"] == "KubernetesExecutor"
    assert kw["config_kwargs"]["spark.x"] == "1"
    assert kw["config_kwargs"]["spark.master"].startswith("k8s://")
    assert kw["executor_config"]["pod_override"].spec.containers[0].name == "base"
    assert callable(kw["pre_execute"])
    assert "executor" not in pyspark_on_k8s(driver=SparkDriverConf(image="d"), executor=SparkExecutorConf(image="e"))

def test_helper_applies_to_stock_decorator(task):
    @task.pyspark
    def job(spark=None):
        return 1
    op = job.override(task_id="j", **pyspark_on_k8s(driver=SparkDriverConf(image="d"), executor=SparkExecutorConf(image="e")))
    with_dag = op  # a DAG is not needed to inspect the operator kwargs
    assert with_dag.kwargs["executor_config"]["pod_override"].metadata.labels["pysparkonk8s/component"] == "driver"
    assert with_dag.kwargs["config_kwargs"]["spark.submit.deployMode"] == "client"

def test_hook_writes_template(tmp_path, monkeypatch):
    import pickle
    from types import SimpleNamespace
    from pysparkonk8s import SparkBaseConf
    for n, v in {"SPARK_DRIVER_POD_NAME": "p", "SPARK_LOCAL_IP": "ip", "SPARK_DRIVER_NAMESPACE": "ns"}.items():
        monkeypatch.setenv(n, v)
    kw = pyspark_on_k8s(base=SparkBaseConf(executor_pod_template_path=str(tmp_path / "t.yaml")),
                        driver=SparkDriverConf(image="d"), executor=SparkExecutorConf(image="e"))
    hook = pickle.loads(pickle.dumps(kw["pre_execute"]))
    hook({"dag": SimpleNamespace(dag_id="d"), "task": SimpleNamespace(task_id="t"), "run_id": "r", "ti": SimpleNamespace(map_index=-1)})
    assert (tmp_path / "t.yaml").exists()
```

`task` is the conftest fixture from Task 1 (resolves `airflow.sdk.task` or `airflow.decorators.task`). The `.override()` result type differs between Airflow 2 and 3; the implementer adjusts the second test to read the stored kwargs the way the installed Airflow exposes them (`op.kwargs` on both as of 2.11 / 3.3; verify).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_helper.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `helper.py` and the package exports**

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/ -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(helper): pyspark_on_k8s override kwargs for the stock decorator"
```

---

### Task 9: `PySparkOnK8sOperator` and `@task.pyspark_on_k8s`

**Files:**
- Create: `src/pysparkonk8s/operators.py`, `src/pysparkonk8s/decorators.py`, `tests/test_operator.py`, `tests/test_decorator.py`
- Modify: `src/pysparkonk8s/__init__.py` (export `PySparkOnK8sOperator`)

**Interfaces:**
- Produces: `class PySparkOnK8sOperator(PySparkOperator)` (upstream `airflow.providers.apache.spark.operators.spark_pyspark.PySparkOperator`): `__init__(self, *, python_callable, driver: SparkDriverConf, executor: SparkExecutorConf, base: SparkBaseConf | None = None, extra=None, op_args=None, op_kwargs=None, **kwargs)`: stores the confs, sets `kwargs["executor_config"] = {**kwargs.get("executor_config", {}), "pod_override": driver.render_pod_specification()}` (a user `pod_override` raises `ValueError`), calls super with `conn_id=None`, `config_kwargs=build_spark_conf(...)`; `ui_color = "#028A0F"`; `execute(self, context)`: `prepare_driver(self.base, self.executor, context)` then `super().execute(context)`. (Upstream builds the session in `execute_callable`, which `execute` calls after templating, so writing the file in `execute` is early enough and keeps the template path independent of upstream internals.)
- `class _PySparkOnK8sDecoratedOperator(DecoratedOperator, PySparkOnK8sOperator)` with `custom_operator_name = "@task.pyspark_on_k8s"`, mirroring upstream's `_PySparkDecoratedOperator` (same `kwargs_to_upstream`, same `SPARK_CONTEXT_KEYS` signature patch; `DecoratedOperator`, `TaskDecorator`, `task_decorator_factory` imported from `airflow.providers.common.compat.sdk`). `def pyspark_on_k8s_task(python_callable=None, multiple_outputs=None, **kwargs) -> TaskDecorator`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_operator.py
import pytest
from types import SimpleNamespace
from pysparkonk8s import PySparkOnK8sOperator, SparkBaseConf, SparkDriverConf, SparkExecutorConf

def test_operator_sets_executor_config_and_conf():
    op = PySparkOnK8sOperator(task_id="t", python_callable=lambda spark: 1, driver=SparkDriverConf(image="d"),
                              executor=SparkExecutorConf(image="e"), extra={"spark.y": "2"},
                              executor_config={"KubernetesExecutor": {"annotations": {"a": "b"}}})
    assert op.executor_config["pod_override"].spec.containers[0].image == "d"
    assert op.executor_config["KubernetesExecutor"] == {"annotations": {"a": "b"}}
    assert op.config_kwargs["spark.y"] == "2"
    assert op.conn_id is None

def test_operator_rejects_user_pod_override():
    with pytest.raises(ValueError, match="pod_override"):
        PySparkOnK8sOperator(task_id="t", python_callable=lambda spark: 1, driver=SparkDriverConf(image="d"),
                             executor=SparkExecutorConf(image="e"), executor_config={"pod_override": object()})

def test_operator_execute_writes_template_then_runs(tmp_path, monkeypatch, mocker):
    for n, v in {"SPARK_DRIVER_POD_NAME": "p", "SPARK_LOCAL_IP": "ip", "SPARK_DRIVER_NAMESPACE": "ns"}.items():
        monkeypatch.setenv(n, v)
    op = PySparkOnK8sOperator(task_id="t", python_callable=lambda spark: 1,
                              base=SparkBaseConf(executor_pod_template_path=str(tmp_path / "t.yaml")),
                              driver=SparkDriverConf(image="d"), executor=SparkExecutorConf(image="e"))
    parent = mocker.patch("pysparkonk8s.operators.PySparkOperator.execute", return_value="ok")
    ctx = {"dag": SimpleNamespace(dag_id="d"), "task": op, "run_id": "r", "ti": SimpleNamespace(map_index=-1)}
    assert op.execute(ctx) == "ok"
    assert (tmp_path / "t.yaml").exists()
    parent.assert_called_once()
```

```python
# tests/test_decorator.py
from pysparkonk8s import SparkDriverConf, SparkExecutorConf

def test_decorator_is_registered_through_providers_manager():
    from airflow.providers_manager import ProvidersManager
    assert "pyspark_on_k8s" in ProvidersManager().taskflow_decorators

def test_task_pyspark_on_k8s(task):
    @task.pyspark_on_k8s(driver=SparkDriverConf(image="d"), executor=SparkExecutorConf(image="e"))
    def job(spark):
        return spark
    op = job.override(task_id="j")
    assert op.kwargs["executor_config"]["pod_override"].spec.containers[0].image == "d"
    assert op.kwargs["config_kwargs"]["spark.kubernetes.executor.container.image"] == "e"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_operator.py tests/test_decorator.py -v`
Expected: FAIL with `ImportError`

- [ ] **Step 3: Implement `operators.py` and `decorators.py`**

The package must be installed (not just on `sys.path`) for the entrypoint test: `uv sync` installs it editable.

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/ -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat: PySparkOnK8sOperator and @task.pyspark_on_k8s"
```

---

### Task 10: Example DAGs and DAG-import test

**Files:**
- Create: `examples/dags/pysparkonk8s_helper_dag.py`, `examples/dags/pysparkonk8s_decorator_dag.py`, `examples/dags/pysparkonk8s_mapped_dag.py`, `tests/test_example_dags.py`

**Interfaces:**
- Each DAG reads `DRIVER_IMAGE` and `EXECUTOR_IMAGE` from env with defaults `"pysparkonk8s-driver:e2e"` and `"spark:4.2.0-python3"`, `schedule=None`, `catchup=False`, tags `["pysparkonk8s"]`. The task does `spark.range(1000).selectExpr("sum(id)").collect()` and returns the sum (`499500`) so XCom proves the session worked. Sizing: driver `cores=1, memory=Memory.mebibytes(1024)`, executor `cores=1, memory=Memory.mebibytes(1024), max_executors=2`. The mapped DAG uses the decorator with `.expand(n=[1, 2])` and returns `n * 499500`. DAG ids `pysparkonk8s_helper`, `pysparkonk8s_decorator`, `pysparkonk8s_mapped`. Import `task` via `from airflow.sdk import task` with `except ImportError: from airflow.decorators import task`; same for `DAG`/`dag`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_example_dags.py
from pathlib import Path

def test_example_dags_import_cleanly():
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "examples" / "dags"), include_examples=False)
    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {"pysparkonk8s_helper", "pysparkonk8s_decorator", "pysparkonk8s_mapped"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_example_dags.py -v`
Expected: FAIL (no DAGs found)

- [ ] **Step 3: Write the three DAG files**

- [ ] **Step 4: Run tests and lint**

Run: `uv run pytest tests/ -v && uv run ruff check . && uv run ruff format --check .`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "docs(examples): helper, decorator and mapped example DAGs"
```

---

### Task 11: RBAC Helm chart

**Files:**
- Create: `charts/pysparkonk8s-rbac/Chart.yaml` (name `pysparkonk8s-rbac`, `version: 1.0.0`, `appVersion: "1.0.0"`, description "Role and RoleBinding that let an Airflow worker pod act as a client-mode Spark driver"), `charts/pysparkonk8s-rbac/values.yaml` (`serviceAccountName: airflow-worker`, `nameOverride: ""`, `labels: {}`, `annotations: {}`), `charts/pysparkonk8s-rbac/templates/role.yaml`, `charts/pysparkonk8s-rbac/templates/rolebinding.yaml`, `charts/pysparkonk8s-rbac/templates/_helpers.tpl`, `charts/pysparkonk8s-rbac/.helmignore`, `tests/test_chart.py`

**Interfaces:**
- Role rules: `pods` (`create, get, list, watch, delete, patch, deletecollection`), `pods/log` (`get, list`), `configmaps` (`create, get, list, watch, delete, patch`), `services` (`create, get, list, delete`). Names `<release>-pysparkonk8s-rbac`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_chart.py
import shutil, subprocess, yaml, pytest

@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_chart_renders_role_for_service_account():
    out = subprocess.check_output(["helm", "template", "af", "charts/pysparkonk8s-rbac", "--set", "serviceAccountName=airflow-worker"])
    docs = {d["kind"]: d for d in yaml.safe_load_all(out) if d}
    assert {"Role", "RoleBinding"} <= docs.keys()
    assert docs["RoleBinding"]["subjects"][0]["name"] == "airflow-worker"
    resources = {r for rule in docs["Role"]["rules"] for r in rule["resources"]}
    assert {"pods", "pods/log", "configmaps", "services"} <= resources
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_chart.py -v`
Expected: FAIL (chart path missing)

- [ ] **Step 3: Write the chart; run `helm lint charts/pysparkonk8s-rbac`**

- [ ] **Step 4: Run test**

Run: `uv run pytest tests/test_chart.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(chart): pysparkonk8s-rbac Role and RoleBinding"
```

---

### Task 12: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`, `matrix.json`

**Interfaces:**
- `matrix.json`: rows `{airflow, python, smoke}`: `("2.11.*", "3.10", true)`, `("2.11.*", "3.12", false)`, `("3.3.*", "3.10", true)`, `("3.3.*", "3.12", false)`, `("3.3.*", "3.14", true)`. Per row: `uv sync`, then `uv pip install "apache-airflow==${AIRFLOW}"` to pin the row's Airflow (the resolver picks provider releases that support the row's Python), then `uv run --no-sync pytest`.
- Jobs: `plan` (reads `matrix.json`; whole workflow skipped on draft PRs through `if: github.event_name != 'pull_request' || !github.event.pull_request.draft` on this job, every other job `needs` it), `lint` (`ruff check`, `ruff format --check`), `test` (matrix from `plan`, `fail-fast: false`; `helm` is preinstalled on ubuntu runners), `docs` (`mkdocs build --strict`, added in Task 14; until then the job runs `true`), `e2e` (Task 13; placeholder `true` for now), `ci-ok` (`needs: [lint, test, docs, e2e]`, `if: always()`, a single `test` step that fails unless every need's `result` is `success`; required status check of the branch protection, and because it always runs a draft PR shows it red). Triggers: `pull_request` `[opened, synchronize, reopened, ready_for_review]`, `workflow_dispatch`, `workflow_call` with boolean input `full` (default true; `false` runs only `smoke` rows). `permissions: contents: read`. Pin actions: `actions/checkout@v7`, `astral-sh/setup-uv@v10` (with `enable-cache: true`), `actions/setup-python@v7`. A header comment explains the draft/`ci-ok` behaviour.

- [ ] **Step 1: Write `matrix.json` and `ci.yml`**

- [ ] **Step 2: Verify locally**

Run: `uv sync && uv pip install "apache-airflow==2.11.*" && uv run --no-sync pytest -q && uv pip install "apache-airflow==3.3.*" && uv run --no-sync pytest -q`
Expected: both PASS. If a test is Airflow-version specific, fix the code (not the test) unless the plan already notes the difference (Task 8 `.override` kwargs access).

- [ ] **Step 3: Commit, push a branch, open a draft PR, mark ready, confirm `ci-ok` green**

```bash
git add -A && git commit -m "ci: lint, Airflow x Python matrix and ci-ok gate"
git push -u origin HEAD
gh pr create --fill --draft && gh pr ready
```

Do not merge yet; later tasks push to the same branch.

---

### Task 13: End-to-end test on kind (Airflow 3)

**Files:**
- Create: `docker/driver.Dockerfile`, `docker/images.json`, `e2e/values.yaml`, `e2e/run.sh`, `e2e/README.md`
- Modify: `.github/workflows/ci.yml` (replace the `e2e` placeholder)

**Interfaces:**
- `docker/driver.Dockerfile` (the Airflow image has no JVM or pyspark and the Spark image has no Airflow, so the driver is always a derived image): `ARG AIRFLOW_VERSION=3.3.2`, `ARG PYTHON_VERSION=3.10`, `ARG PYSPARK_VERSION=4.2.0` (all three before the first `FROM`, since the first two are used in it); `FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}`; as root `apt-get install -y --no-install-recommends openjdk-21-jre-headless procps` and `rm -rf /var/lib/apt/lists/*`; `ENV JAVA_HOME` set from `$(dirname $(dirname $(readlink -f $(which java))))` at build time (so arm64 works too), `ENV PATH=$JAVA_HOME/bin:$PATH`; as airflow `COPY dist/*.whl /tmp/wheels/` and `pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}" /tmp/wheels/*.whl`; OCI labels `org.opencontainers.image.source=https://github.com/sdaberdaku/pysparkonk8s`, `org.opencontainers.image.licenses=Apache-2.0`. No DAGs in the image: the e2e mounts `examples/dags` through the chart's `dags.persistence`? No: simpler, the e2e builds a second local image `FROM pysparkonk8s-driver:e2e` + `COPY examples/dags /opt/airflow/dags` with an inline Dockerfile in `run.sh`.
- `docker/images.json` as in Global Constraints; consumed by the `release.yml` image matrix.
- `e2e/values.yaml` for chart `apache-airflow/airflow` `1.22.0`: `executor: KubernetesExecutor`, `airflowVersion: 3.3.2`, `defaultAirflowRepository: pysparkonk8s-driver`, `defaultAirflowTag: e2e`, `images.airflow.pullPolicy: Never`, `dags.persistence.enabled: false`, `dags.gitSync.enabled: false`, `workers.resources` small, `config.core.load_examples: "False"`, `config.kubernetes_executor.worker_container_repository/tag` same image, env `DRIVER_IMAGE=pysparkonk8s-driver:e2e`, `EXECUTOR_IMAGE=spark:4.2.0-python3` on scheduler and in `config.kubernetes_environment_variables`. Keep the chart's default user (`admin`/`admin`).
- `e2e/run.sh` (bash, `set -euo pipefail`; env `PYTHON_VERSION` default `3.10`, `EXECUTOR_IMAGE` default `spark:4.2.0-python3`): `uv build`; `docker build -f docker/driver.Dockerfile --build-arg PYTHON_VERSION=$PYTHON_VERSION -t pysparkonk8s-driver:base .`; build `pysparkonk8s-driver:e2e` from it with the example DAGs copied in; `docker pull $EXECUTOR_IMAGE` (the CI 3.14 row passes `EXECUTOR_IMAGE=ghcr.io/sdaberdaku/spark:4.2.0-python3.14`); `kind create cluster --name pysparkonk8s` (skip if exists); `kind load docker-image` both images; `helm upgrade --install airflow apache-airflow/airflow --version 1.22.0 -n airflow --create-namespace -f e2e/values.yaml --wait --timeout 10m`; `helm upgrade --install pysparkonk8s-rbac charts/pysparkonk8s-rbac -n airflow --set serviceAccountName=airflow-worker`; port-forward the api-server (`svc/airflow-api-server 8080`); obtain a token `POST /auth/token {"username":"admin","password":"admin"}`; for each of the three DAG ids: unpause (`PATCH /api/v2/dags/{id}` `{"is_paused": false}`), trigger (`POST /api/v2/dags/{id}/dagRuns`), poll `GET /api/v2/dags/{id}/dagRuns/{run_id}` until `state` in `success|failed` (timeout 15 min); on failure dump `kubectl -n airflow get pods -A`, scheduler logs and the task logs from the API, exit 1; after all succeed assert `kubectl -n airflow get pods -l pysparkonk8s/component=executor --no-headers | wc -l` is `0`; assert XCom `return_value` of `pysparkonk8s_helper` task `job` equals `499500` via `GET /api/v2/dags/{id}/dagRuns/{run_id}/taskInstances/job/xcomEntries/return_value`.
- CI job `e2e`: matrix `include: [{python: "3.10", executor: "spark:4.2.0-python3"}, {python: "3.14", executor: "ghcr.io/sdaberdaku/spark:4.2.0-python3.14"}]` (`fail-fast: false`), `runs-on: ubuntu-latest`, `timeout-minutes: 40`, `helm/kind-action@v1` with `install_only: true`, `astral-sh/setup-uv@v10`, `helm repo add apache-airflow https://airflow.apache.org`, `PYTHON_VERSION=${{ matrix.python }} EXECUTOR_IMAGE=${{ matrix.executor }} bash e2e/run.sh`; on `failure()` upload `kubectl` dumps as an artifact. The `values.yaml` `EXECUTOR_IMAGE` env is passed from `run.sh` with `--set`.

- [ ] **Step 1: Write the four files and the CI job**

- [ ] **Step 2: Run locally**

Run: `bash e2e/run.sh`
Expected: three DAG runs `success`, zero executor pods left, XCom `499500`. Fix the code when a run fails; the task is done only with a green local run.

- [ ] **Step 3: Push, confirm the `e2e` job is green on the PR**

```bash
git add -A && git commit -m "test(e2e): kind cluster with Airflow 3 KubernetesExecutor"
git push
```

---

### Task 14: Documentation site

**Files:**
- Create: `mkdocs.yml` (`site_name: pysparkonk8s`, `site_description: Ephemeral Spark clusters for @task.pyspark on Kubernetes`, `site_url: https://sdaberdaku.github.io/pysparkonk8s/`, `repo_url`, `edit_uri: edit/main/docs/site/`, `docs_dir: docs/site`, `exclude_docs: requirements.txt`, theme `material` with light/dark palette toggle, features `content.code.copy` and `content.action.edit`, plugin `search`, markdown extension `attr_list`, nav below), `docs/site/requirements.txt` (`mkdocs==1.6.1`, `mkdocs-material==9.7.7`), `docs/site/{index,quickstart,configuration,how-it-works,examples,operations,limitations,development,release}.md`, `.github/workflows/pages.yml` (on push to `main` touching `docs/site/**`, `mkdocs.yml`, `CHANGELOG.md`, `README.md`, the workflow itself, plus `workflow_dispatch`; `concurrency: pages`; job `build`: checkout, `actions/setup-python@v7` 3.12 with pip cache on `docs/site/requirements.txt`, `pip install -r docs/site/requirements.txt`, `cp CHANGELOG.md docs/site/changelog.md`, `mkdocs build --strict`, `actions/upload-pages-artifact@v5` with `path: site`; job `deploy`: `needs: build`, `permissions: pages: write, id-token: write`, environment `github-pages`, `actions/deploy-pages@v5`), `README.md` (full)
- Modify: `.github/workflows/ci.yml` `docs` job (`pip install -r docs/site/requirements.txt`, copy changelog, `mkdocs build --strict`)

**Interfaces:**
- Nav: Home, Quick start, Configuration, Examples, How it works, Operations, Limitations, Development, Release, Changelog.
- Content requirements (plain prose, every claim true of the code as built):
  - index: what it does in three sentences, the two entry points side by side (10-line snippets), links.
  - quickstart: RBAC chart install (`helm install pysparkonk8s-rbac oci://ghcr.io/sdaberdaku/charts/pysparkonk8s-rbac --version 1.0.0 -n airflow --set serviceAccountName=<worker SA>`), the published driver image `ghcr.io/sdaberdaku/pysparkonk8s/driver:1.0.0-airflow3.3.2-python3.12` as the worker image with executor `ghcr.io/sdaberdaku/spark:4.2.0-python3.12` (table of driver tags and the matching executor image per Python, the 3.10 row paired with stock `spark:4.2.0-python3`; `docker/driver.Dockerfile` with other build args for another Airflow/Python), `pip install airflow-provider-pysparkonk8s` when building one's own image, first DAG with the helper, what to expect in the UI and in `kubectl get pods`.
  - configuration: one table per conf object: field, default, Spark key(s) it renders, pod field it renders; the memory sizing rules (request = heap + overhead, overhead factor 0.4 driver / 0.1 executor, floor 384Mi, executor limit set by Spark, `pyspark_memory` added), `extra` and the `None` unset rule, the Jinja rule (conf values templated, pod fields static).
  - examples: dynamic allocation, fixed executors, `same_zone_as`, `one_per_node` per run vs cluster, tolerations and node selector, volumes, env vars, Jinja in `extra`, mapped tasks, multi-executor deployments (`executor_name`), running the same task on Spark Connect vs on-demand.
  - how-it-works: worker pod as driver (`pod_override`), the three Downward API env vars and `${env:...}`, template write at `pre_execute` / `execute`, KubernetesExecutor labels and the follow-driver affinity, lifecycle (session stop deletes executors; `deleteOnTermination`), why the stock decorator name cannot be overridden (ProvidersManager keeps the first registration).
  - operations: RBAC rules and why configmaps, ports 7078/7079 and NetworkPolicy, image requirements (JRE, pyspark version equal to the executor's Spark version, Python minor equal on both sides, what the driver image contains, where the executor images come from and how to add jars on top), where executor logs go, Spark UI off by default, troubleshooting table (missing env vars error, executors Pending with required affinity, OOMKilled driver, image pull).
  - limitations: client mode only, Spark 4 only, Airflow 2.11+, pod fields not templatable, one task per driver pod, no cluster mode, decorator name.
  - development: `uv sync`, `uv run pytest`, `ruff`, running the e2e locally (kind, docker), matrix.
  - release: tag `vX.Y.Z` after bumping `__version__` and the chart version, what the workflow publishes (PyPI, OCI chart, GitHub release), PyPI trusted publisher settings.
  - README: short version of index + quickstart + links to the site, badges for PyPI and CI.

- [ ] **Step 1: Write the files**

- [ ] **Step 2: Build strictly**

Run: `pip install -r docs/site/requirements.txt && cp CHANGELOG.md docs/site/changelog.md && mkdocs build --strict`
Expected: no warnings

- [ ] **Step 3: Commit and push, confirm `docs` job green**

```bash
git add -A && git commit -m "docs: mkdocs site, pages workflow and README"
git push
```

---

### Task 15: Release workflow, branch protection, merge

**Files:**
- Create: `.github/workflows/release.yml`
- Modify: `CHANGELOG.md` (`## [1.0.0] - <date>` with the feature list), `README.md` (nothing if already complete)

**Interfaces:**
- `release.yml` on `push: tags: ["v[0-9]*"]`: job `test` calls `ci.yml` with `full: true`; job `publish` (`needs: test`, `permissions: contents: write, id-token: write, packages: write`, `environment: pypi`): checkout, setup-uv, `test "v$(uv version --short)" = "$GITHUB_REF_NAME"` (fails on a tag that does not match `__version__`), `uv build`, upload `dist/` as an artifact, `pypa/gh-action-pypi-publish@release/v1` (trusted publishing, no token), `helm package charts/pysparkonk8s-rbac` and `helm push *.tgz oci://ghcr.io/sdaberdaku/charts` after `helm registry login ghcr.io -u $GITHUB_ACTOR --password-stdin <<< $GITHUB_TOKEN`, `gh release create "$GITHUB_REF_NAME" dist/* --generate-notes`. Job `image` (`needs: publish`, `permissions: contents: read, packages: write`, matrix from `docker/images.json`): download the `dist/` artifact, `docker/setup-qemu-action@v4`, `docker/setup-buildx-action@v4`, `docker/login-action@v4` to `ghcr.io` with `GITHUB_TOKEN`, `docker/build-push-action@v7` with `file: docker/driver.Dockerfile`, `platforms: linux/amd64,linux/arm64`, build args from the row, tags `ghcr.io/sdaberdaku/pysparkonk8s/driver:${VERSION}-airflow${airflow}-python${python}` plus, when the row has `latest: true`, `:${VERSION}` and `:latest`; `cache-from/to: type=gha`.
- CI (`ci.yml`): the `e2e` job already builds `docker/Dockerfile` for amd64 through `e2e/run.sh`; no push on pull requests.
- Branch protection, applied with `gh api -X PUT repos/sdaberdaku/pysparkonk8s/branches/main/protection` with body: `required_status_checks: {strict: true, contexts: ["ci-ok"]}`, `enforce_admins: true`, `required_pull_request_reviews: null`, `restrictions: null`, `required_linear_history: true`, `allow_force_pushes: true`, `allow_deletions: false`. Repo settings: squash and rebase merges only (`gh repo edit --enable-merge-commit=false`), delete branch on merge, Pages source GitHub Actions.
- Manual steps for the owner (write them in `docs/site/release.md` and say them in the final report): before tagging, add a pending trusted publisher on pypi.org for project `airflow-provider-pysparkonk8s`, owner `sdaberdaku`, repository `pysparkonk8s`, workflow `release.yml`, environment `pypi`, and create the `pypi` environment in the repo settings. After the first release the GHCR packages `pysparkonk8s/driver` and `charts/pysparkonk8s-rbac` must be set to public and linked to the repository (package settings page; no API for visibility).

- [ ] **Step 1: Write `release.yml`, update `CHANGELOG.md`**

- [ ] **Step 2: Validate the workflow file**

Run: `uvx --from actionlint-py actionlint .github/workflows/*.yml` (or `actionlint` if installed)
Expected: no findings

- [ ] **Step 3: Commit, push, wait for `ci-ok`, apply protection, merge**

```bash
git add -A && git commit -m "ci: release workflow for PyPI, GHCR chart and GitHub release"
git push
gh pr checks --watch
gh api -X PUT repos/sdaberdaku/pysparkonk8s/branches/main/protection --input protection.json   # body as above; file in the scratchpad, not the repo
gh repo edit sdaberdaku/pysparkonk8s --enable-merge-commit=false --delete-branch-on-merge
gh pr merge --rebase
```

- [ ] **Step 4: Report** the merge SHA, the Pages URL once `pages.yml` ran, and the two manual steps (PyPI trusted publisher, `pypi` environment) that must precede `git tag v1.0.0 && git push --tags`. After 1.0.0 is on PyPI (owner's step): archive `sdaberdaku/apache-airflow-providers-pysparkonk8s` with a README pointer to the new repo.

---

### Task 16: Renovate configuration

**Files:**
- Create: `renovate.json`
- Modify: `docs/site/development.md` (one paragraph), `README.md` (nothing)

**Interfaces:**
- `renovate.json` (schema `https://docs.renovatebot.com/renovate-schema.json`): `extends: ["config:recommended", ":dependencyDashboard"]`; `enabledManagers: ["pep621", "dockerfile", "github-actions", "custom.regex"]` plus `lockFileMaintenance: {enabled: true, schedule: ["before 6am on monday"]}` so `uv.lock` is refreshed; custom regex managers: (a) `docker/images.json` `"airflow": "(?<currentValue>[^"]+)"` → datasource `docker`, depName `apache/airflow`, versioning `docker`; (b) same file `"pyspark": "(?<currentValue>[^"]+)"` → datasource `pypi`, depName `pyspark`; (c) `e2e/run.sh` (or wherever the chart version literal lives) `--version (?<currentValue>[0-9.]+)` → datasource `helm`, depName `airflow`, registryUrl `https://airflow.apache.org`; (d) `e2e/run.sh` executor image literal `spark:(?<currentValue>[0-9.]+)-python3` → datasource `docker`, depName `spark`; `packageRules`: `{matchUpdateTypes: ["patch", "pin", "digest"], automerge: true}`, `{matchManagers: ["github-actions"], automerge: true}`, `{matchDepNames: ["apache/airflow"], matchUpdateTypes: ["major"], enabled: false}` (Airflow majors are a code decision), `{matchPackageNames: ["apache-airflow"], matchUpdateTypes: ["major"], enabled: false}`; `platformAutomerge: true`; `schedule: ["before 6am on monday"]`; `prConcurrentLimit: 4`; `labels: ["dependencies"]`.
- development.md paragraph: Renovate opens the update PRs; patch and action updates merge on their own once CI passes; the Mend Renovate app must be installed on the repository for the config to act.

- [ ] **Step 1: Write and validate** (`npx --yes renovate-config-validator renovate.json`, or the `renovate/renovate` container)
- [ ] **Step 2: Commit `ci: Renovate configuration`, push, PR, `ci-ok`, squash-merge**
- [ ] **Step 3: Report** the merge commit.
