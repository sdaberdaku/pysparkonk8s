"""Spark-on-Kubernetes configuration objects that render to Spark conf and pod specs."""

from __future__ import annotations

from dataclasses import dataclass

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
