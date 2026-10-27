"""Spark-on-Kubernetes configuration objects that render to Spark conf and pod specs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from pysparkonk8s.resources import CPU, Memory

if TYPE_CHECKING:
    from kubernetes.client import models as k8s

DRIVER_ENV_POD_NAME = "SPARK_DRIVER_POD_NAME"
DRIVER_ENV_LOCAL_IP = "SPARK_LOCAL_IP"
DRIVER_ENV_NAMESPACE = "SPARK_DRIVER_NAMESPACE"
COMPONENT_LABEL = "pysparkonk8s/component"
DRIVER_COMPONENT = "driver"
EXECUTOR_COMPONENT = "executor"


@dataclass(kw_only=True)
class SparkBaseConf:
    """Cluster-level settings shared by the driver and its executors."""

    master: str = "k8s://https://kubernetes.default.svc.cluster.local:443"
    namespace: str | None = None
    image_pull_policy: str = "IfNotPresent"
    image_pull_secrets: str | None = None
    driver_port: int = 7078
    block_manager_port: int = 7079
    delete_executors_on_termination: bool = True
    executor_pod_template_path: str = "/tmp/pysparkonk8s/executor-pod-template.yaml"
    ca_cert_file: str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    oauth_token_file: str = "/var/run/secrets/kubernetes.io/serviceaccount/token"

    def render_spark_conf(self) -> dict[str, str]:
        """Return the Spark conf entries; driver identity values are resolved by Spark from env vars."""
        conf = {
            "spark.master": self.master,
            "spark.submit.deployMode": "client",
            "spark.kubernetes.authenticate.submission.caCertFile": self.ca_cert_file,
            "spark.kubernetes.authenticate.submission.oauthTokenFile": self.oauth_token_file,
            "spark.kubernetes.container.image.pullPolicy": self.image_pull_policy,
            "spark.kubernetes.executor.deleteOnTermination": str(self.delete_executors_on_termination).lower(),
            "spark.driver.port": str(self.driver_port),
            "spark.blockManager.port": str(self.block_manager_port),
            "spark.kubernetes.driver.pod.name": f"${{env:{DRIVER_ENV_POD_NAME}}}",
            "spark.driver.host": f"${{env:{DRIVER_ENV_LOCAL_IP}}}",
            "spark.kubernetes.namespace": self.namespace or f"${{env:{DRIVER_ENV_NAMESPACE}}}",
            "spark.kubernetes.executor.podTemplateFile": self.executor_pod_template_path,
        }
        if self.image_pull_secrets:
            conf["spark.kubernetes.container.image.pullSecrets"] = self.image_pull_secrets
        return conf


def _clamp_up(limit, request):
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
        return self.request_cpu or self.cores, memory, self.request_ephemeral_storage

    def effective_limits(self) -> tuple[CPU | None, Memory | None, Memory | None]:
        """Return (cpu, memory, ephemeral storage) limits, each clamped up to its request."""
        cpu, memory, ephemeral = self.effective_requests()
        return (
            _clamp_up(self.limit_cpu, cpu),
            _clamp_up(self.limit_memory, memory) or memory,
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

    @abstractmethod
    def _render_affinity(self, run: RunIdentity | None) -> Any: ...  # noqa: F821 (RunIdentity: Task 5)

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

    def render_pod_specification(self, run: RunIdentity | None = None) -> k8s.V1Pod:  # noqa: F821
        from kubernetes.client import models as k8s

        return k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                labels={COMPONENT_LABEL: self.component, **self.pod_labels},
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

    def _render_affinity(self, run: RunIdentity | None) -> Any:  # noqa: F821 (RunIdentity: Task 5)
        return self.affinity
