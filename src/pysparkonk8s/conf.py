"""Spark-on-Kubernetes configuration objects that render to Spark conf and pod specs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

from pysparkonk8s.resources import CPU, Memory

if TYPE_CHECKING:
    from kubernetes.client import models as k8s

DRIVER_ENV_POD_NAME = "SPARK_DRIVER_POD_NAME"
DRIVER_ENV_LOCAL_IP = "SPARK_LOCAL_IP"
DRIVER_ENV_NAMESPACE = "SPARK_DRIVER_NAMESPACE"
COMPONENT_LABEL = "pysparkonk8s/component"
DRIVER_COMPONENT = "driver"
EXECUTOR_COMPONENT = "executor"
HOSTNAME_TOPOLOGY = "kubernetes.io/hostname"
ZONE_TOPOLOGY = "topology.kubernetes.io/zone"

_Sized = TypeVar("_Sized", CPU, Memory)


class _Sentinel(Enum):
    FOLLOW_DRIVER = "FOLLOW_DRIVER"


FOLLOW_DRIVER = _Sentinel.FOLLOW_DRIVER


@dataclass(kw_only=True)
class SparkBaseConf:
    """Cluster-level settings shared by the driver and its executors."""

    master: str = "k8s://https://kubernetes.default.svc:443"
    namespace: str | None = None
    image_pull_policy: str = "IfNotPresent"
    image_pull_secrets: str | None = None
    driver_port: int = 7078
    block_manager_port: int = 7079
    delete_executors_on_termination: bool = True
    executor_pod_template_path: str = "/tmp/pysparkonk8s/executor-pod-template.yaml"
    ca_cert_file: str | None = None
    oauth_token_file: str | None = None

    def render_spark_conf(self) -> dict[str, str]:
        """Return the Spark conf entries; driver identity values are resolved by Spark from env vars."""
        conf = {
            "spark.master": self.master,
            "spark.submit.deployMode": "client",
            "spark.kubernetes.container.image.pullPolicy": self.image_pull_policy,
            "spark.kubernetes.executor.deleteOnTermination": str(self.delete_executors_on_termination).lower(),
            "spark.driver.port": str(self.driver_port),
            "spark.blockManager.port": str(self.block_manager_port),
            "spark.kubernetes.driver.pod.name": f"${{env:{DRIVER_ENV_POD_NAME}}}",
            "spark.driver.host": f"${{env:{DRIVER_ENV_LOCAL_IP}}}",
            "spark.kubernetes.namespace": self.namespace or f"${{env:{DRIVER_ENV_NAMESPACE}}}",
            "spark.kubernetes.executor.podTemplateFile": self.executor_pod_template_path,
        }
        if self.ca_cert_file:
            conf["spark.kubernetes.authenticate.caCertFile"] = self.ca_cert_file
        if self.oauth_token_file:
            conf["spark.kubernetes.authenticate.oauthTokenFile"] = self.oauth_token_file
        if self.image_pull_secrets:
            conf["spark.kubernetes.container.image.pullSecrets"] = self.image_pull_secrets
        return conf


@dataclass(frozen=True)
class RunIdentity:
    """Airflow task-instance identity; values are already label-safe."""

    dag_id: str
    task_id: str
    run_id: str
    map_index: int | None = None

    @classmethod
    def from_context(cls, context: Mapping) -> RunIdentity:
        """Build the identity from an Airflow task context, applying KubernetesExecutor's label sanitising."""
        from airflow.providers.cncf.kubernetes.pod_generator import make_safe_label_value

        map_index = context["ti"].map_index
        return cls(
            dag_id=make_safe_label_value(context["dag"].dag_id),
            task_id=make_safe_label_value(context["task"].task_id),
            run_id=make_safe_label_value(context["run_id"]),
            map_index=None if map_index is None or map_index < 0 else map_index,
        )

    def labels(self) -> dict[str, str]:
        """The labels KubernetesExecutor stamps on the worker pod (the Spark driver)."""
        labels = {"dag_id": self.dag_id, "task_id": self.task_id, "run_id": self.run_id}
        if self.map_index is not None:
            labels["map_index"] = str(self.map_index)
        return labels


@dataclass(frozen=True)
class PlacementRule:
    """Required node and pod placement; combine rules with ``&``."""

    zone: str | None = None
    anti_affinity_per_run: bool | None = None  # None: no anti-affinity

    def __and__(self, other: PlacementRule) -> PlacementRule:
        return PlacementRule(
            zone=self.zone if self.zone is not None else other.zone,
            anti_affinity_per_run=(
                self.anti_affinity_per_run if self.anti_affinity_per_run is not None else other.anti_affinity_per_run
            ),
        )

    def render(self, run: RunIdentity | None) -> k8s.V1Affinity:
        from kubernetes.client import models as k8s

        node_affinity = pod_anti_affinity = None
        if self.zone is not None:
            node_affinity = k8s.V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                    node_selector_terms=[
                        k8s.V1NodeSelectorTerm(
                            match_expressions=[
                                k8s.V1NodeSelectorRequirement(key=ZONE_TOPOLOGY, operator="In", values=[self.zone])
                            ]
                        )
                    ]
                )
            )
        if self.anti_affinity_per_run is not None:
            if self.anti_affinity_per_run and run is None:
                raise ValueError("per-run placement rules apply to executors only")
            pod_anti_affinity = k8s.V1PodAntiAffinity(
                required_during_scheduling_ignored_during_execution=[
                    k8s.V1PodAffinityTerm(
                        topology_key=HOSTNAME_TOPOLOGY,
                        label_selector=k8s.V1LabelSelector(
                            match_expressions=[
                                k8s.V1LabelSelectorRequirement(
                                    key=COMPONENT_LABEL, operator="In", values=[DRIVER_COMPONENT, EXECUTOR_COMPONENT]
                                )
                            ],
                            match_labels=run.labels() if self.anti_affinity_per_run else None,
                        ),
                    )
                ]
            )
        return k8s.V1Affinity(node_affinity=node_affinity, pod_anti_affinity=pod_anti_affinity)


def same_zone_as(zone: str) -> PlacementRule:
    """Require nodes in ``zone``."""
    return PlacementRule(zone=zone)


def one_per_node(per_run: bool = True) -> PlacementRule:
    """Keep Spark pods of one run (or of any run, when ``per_run`` is false) on separate nodes."""
    return PlacementRule(anti_affinity_per_run=per_run)


def follow_driver(run: RunIdentity) -> k8s.V1Affinity:
    """Prefer the run's driver node, then its executors' nodes, then the same zone."""
    from kubernetes.client import models as k8s

    def term(weight: int, topology_key: str, component: str) -> k8s.V1WeightedPodAffinityTerm:
        return k8s.V1WeightedPodAffinityTerm(
            weight=weight,
            pod_affinity_term=k8s.V1PodAffinityTerm(
                topology_key=topology_key,
                label_selector=k8s.V1LabelSelector(match_labels={**run.labels(), COMPONENT_LABEL: component}),
            ),
        )

    return k8s.V1Affinity(
        pod_affinity=k8s.V1PodAffinity(
            preferred_during_scheduling_ignored_during_execution=[
                term(100, HOSTNAME_TOPOLOGY, DRIVER_COMPONENT),
                term(75, HOSTNAME_TOPOLOGY, EXECUTOR_COMPONENT),
                term(50, ZONE_TOPOLOGY, DRIVER_COMPONENT),
                term(25, ZONE_TOPOLOGY, EXECUTOR_COMPONENT),
            ]
        )
    )


def _clamp_up(limit: _Sized | None, request: _Sized | None) -> _Sized | None:
    """Raise ``limit`` to ``request`` when it is below it; ``None`` limits stay unset."""
    return limit if limit is None or request is None else max(limit, request)


@dataclass(kw_only=True)
class _K8sConf(ABC):
    """Pod shape and sizing shared by the driver and executors."""

    MIN_OVERHEAD: ClassVar[Memory] = Memory.mebibytes(384)
    component: ClassVar[str]
    container_name: ClassVar[str]
    default_overhead_factor: ClassVar[float]

    image: str
    cores: CPU | None = CPU.cores(1)
    memory: Memory | None = Memory.gibibytes(1)
    memory_overhead: Memory | None = None
    memory_overhead_factor: float | None = None
    request_cpu: CPU | None = None
    limit_cpu: CPU | None = None
    request_memory: Memory | None = None
    limit_memory: Memory | None = None
    request_ephemeral_storage: Memory | None = None
    limit_ephemeral_storage: Memory | None = None
    extra_java_options: str | None = "-XX:+ExitOnOutOfMemoryError"
    extra_class_path: str | None = None
    extra_library_path: str | None = None
    service_account_name: str | None = None
    tolerations: list | None = None
    node_selector: dict[str, str] | None = None
    affinity: Any | None = None
    volumes: list | None = None
    volume_mounts: list | None = None
    env: list | None = None
    pod_labels: dict[str, str] = field(default_factory=dict)
    pod_annotations: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.image:
            raise ValueError("image is required")
        if self.cores is not None and self.cores.milli % 1000 != 0:
            raise ValueError(
                f"cores must be a whole number of cores, got {self.cores.to_spark_spec()}; "
                "use request_cpu for a fractional pod CPU request"
            )

    def effective_memory_overhead(self) -> Memory | None:
        if self.memory is None:
            return None
        if self.memory_overhead is not None:
            return self.memory_overhead
        factor = self.default_overhead_factor if self.memory_overhead_factor is None else self.memory_overhead_factor
        return max(self.memory * factor, self.MIN_OVERHEAD)

    def effective_requests(self) -> tuple[CPU | None, Memory | None, Memory | None]:
        """Return (cpu, memory, ephemeral storage) requests with defaults applied."""
        memory = self.request_memory
        if memory is None and self.memory is not None:
            memory = self.memory + self.effective_memory_overhead()
        cpu = self.cores if self.request_cpu is None else self.request_cpu
        return cpu, memory, self.request_ephemeral_storage

    def effective_limits(self) -> tuple[CPU | None, Memory | None, Memory | None]:
        """Return (cpu, memory, ephemeral storage) limits, each clamped up to its request."""
        cpu, memory, ephemeral = self.effective_requests()
        limit_memory = _clamp_up(self.limit_memory, memory)
        return (
            _clamp_up(self.limit_cpu, cpu),
            memory if limit_memory is None else limit_memory,
            _clamp_up(self.limit_ephemeral_storage, ephemeral),
        )

    def render_spark_conf(self) -> dict[str, str]:
        prefix = f"spark.{self.component}"
        conf: dict[str, str] = {}
        if self.cores is not None:
            conf[f"{prefix}.cores"] = self.cores.to_spark_spec()
        if self.memory is not None:
            conf[f"{prefix}.memory"] = self.memory.to_spark_spec()
            conf[f"{prefix}.memoryOverhead"] = self.effective_memory_overhead().to_spark_spec()
        for key, value in (
            ("extraJavaOptions", self.extra_java_options),
            ("extraClassPath", self.extra_class_path),
            ("extraLibraryPath", self.extra_library_path),
        ):
            if value is not None:
                conf[f"{prefix}.{key}"] = value
        return conf

    @abstractmethod
    def _render_env(self) -> list | None: ...

    def _render_affinity(self, run: RunIdentity | None) -> Any:
        if isinstance(self.affinity, PlacementRule):
            return self.affinity.render(run)
        return self.affinity

    def _render_labels(self, run: RunIdentity | None) -> dict[str, str]:
        return {**self.pod_labels, COMPONENT_LABEL: self.component}

    def _render_resources(self) -> k8s.V1ResourceRequirements:
        from kubernetes.client import models as k8s

        def quantities(cpu, memory, ephemeral):
            values = {
                "cpu": cpu,
                "memory": memory,
                "ephemeral-storage": ephemeral,
            }
            return {name: v.to_k8s_spec() for name, v in values.items() if v is not None} or None

        return k8s.V1ResourceRequirements(
            requests=quantities(*self.effective_requests()),
            limits=quantities(*self.effective_limits()),
        )

    def render_pod_specification(self, run: RunIdentity | None = None) -> k8s.V1Pod:
        from kubernetes.client import models as k8s

        return k8s.V1Pod(
            api_version="v1",
            kind="Pod",
            metadata=k8s.V1ObjectMeta(
                labels=self._render_labels(run),
                annotations=self.pod_annotations or None,
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name=self.container_name,
                        image=self.image,
                        env=self._render_env(),
                        resources=self._render_resources(),
                        volume_mounts=self.volume_mounts,
                    )
                ],
                service_account_name=self.service_account_name,
                tolerations=self.tolerations,
                node_selector=self.node_selector,
                affinity=self._render_affinity(run),
                volumes=self.volumes,
            ),
        )


@dataclass(kw_only=True)
class SparkDriverConf(_K8sConf):
    """The Airflow worker pod that hosts the Spark driver (client mode)."""

    component: ClassVar[str] = DRIVER_COMPONENT
    container_name: ClassVar[str] = "base"
    default_overhead_factor: ClassVar[float] = 0.4

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.affinity is FOLLOW_DRIVER:
            raise ValueError("FOLLOW_DRIVER applies to executors only")

    def _render_env(self) -> list:
        from kubernetes.client import models as k8s

        def downward(name: str, path: str) -> k8s.V1EnvVar:
            return k8s.V1EnvVar(
                name=name, value_from=k8s.V1EnvVarSource(field_ref=k8s.V1ObjectFieldSelector(field_path=path))
            )

        return [
            *(self.env or []),
            downward(DRIVER_ENV_POD_NAME, "metadata.name"),
            downward(DRIVER_ENV_LOCAL_IP, "status.podIP"),
            downward(DRIVER_ENV_NAMESPACE, "metadata.namespace"),
        ]


@dataclass(kw_only=True)
class SparkExecutorConf(_K8sConf):
    """Executor pods, rendered into the pod template file Spark reads."""

    component: ClassVar[str] = EXECUTOR_COMPONENT
    container_name: ClassVar[str] = "spark-kubernetes-executor"
    default_overhead_factor: ClassVar[float] = 0.1

    instances: int | None = None
    dynamic_allocation: bool = True
    min_executors: int = 0
    max_executors: int = 10
    allocation_batch_size: int | None = None
    shuffle_tracking: bool = True
    pyspark_memory: Memory | None = None
    affinity: Any | None = FOLLOW_DRIVER

    def effective_requests(self) -> tuple[CPU | None, Memory | None, Memory | None]:
        cpu, memory, ephemeral = super().effective_requests()
        if self.request_memory is None and memory is not None and self.pyspark_memory is not None:
            memory = memory + self.pyspark_memory
        return cpu, memory, ephemeral

    def render_spark_conf(self) -> dict[str, str]:
        conf = super().render_spark_conf()
        conf["spark.kubernetes.executor.container.image"] = self.image
        conf["spark.kubernetes.executor.podTemplateContainerName"] = self.container_name
        if self.service_account_name:
            conf["spark.kubernetes.authenticate.executor.serviceAccountName"] = self.service_account_name
        cpu, _, _ = self.effective_requests()
        if cpu is not None:
            conf["spark.kubernetes.executor.request.cores"] = cpu.to_spark_spec()
        limit_cpu, _, _ = self.effective_limits()
        if limit_cpu is not None:
            conf["spark.kubernetes.executor.limit.cores"] = limit_cpu.to_spark_spec()
        if self.pyspark_memory is not None:
            conf["spark.executor.pyspark.memory"] = self.pyspark_memory.to_spark_spec()
        conf["spark.dynamicAllocation.enabled"] = str(self.dynamic_allocation).lower()
        if self.dynamic_allocation:
            conf["spark.dynamicAllocation.shuffleTracking.enabled"] = str(self.shuffle_tracking).lower()
            conf["spark.dynamicAllocation.minExecutors"] = str(self.min_executors)
            conf["spark.dynamicAllocation.maxExecutors"] = str(self.max_executors)
        elif self.instances is not None:
            conf["spark.executor.instances"] = str(self.instances)
        if self.allocation_batch_size is not None:
            conf["spark.kubernetes.allocation.batch.size"] = str(self.allocation_batch_size)
        return conf

    def render_pod_specification(self, run: RunIdentity | None = None) -> k8s.V1Pod:
        if run is None:
            raise ValueError("executor pod templates need the run identity")
        return super().render_pod_specification(run)

    def _render_env(self) -> list | None:
        return self.env

    def _render_labels(self, run: RunIdentity | None) -> dict[str, str]:
        return {**run.labels(), **self.pod_labels, COMPONENT_LABEL: self.component}

    def _render_affinity(self, run: RunIdentity | None) -> Any:
        if self.affinity is FOLLOW_DRIVER:
            return follow_driver(run)
        return super()._render_affinity(run)


def build_spark_conf(
    base: SparkBaseConf,
    driver: SparkDriverConf,
    executor: SparkExecutorConf,
    extra: Mapping[str, str | None] | None = None,
) -> dict[str, str]:
    """Merge base, driver and executor Spark conf, then apply ``extra`` (``None`` removes a key).
    Later sources win on key clashes."""
    conf = {**base.render_spark_conf(), **driver.render_spark_conf(), **executor.render_spark_conf()}
    for key, value in (extra or {}).items():
        if value is None:
            conf.pop(key, None)
        else:
            conf[key] = value
    return conf
