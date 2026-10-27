"""Write the executor pod template inside the driver pod, before the Spark session starts."""

from __future__ import annotations

import os
from collections.abc import Mapping

from pysparkonk8s.conf import (
    DRIVER_ENV_LOCAL_IP,
    DRIVER_ENV_NAMESPACE,
    DRIVER_ENV_POD_NAME,
    RunIdentity,
    SparkBaseConf,
    SparkExecutorConf,
)
from pysparkonk8s.errors import PySparkOnK8sError


def check_driver_env(base: SparkBaseConf) -> None:
    """Raise if the driver identity env vars set by the driver pod_override are missing."""
    required = [DRIVER_ENV_POD_NAME, DRIVER_ENV_LOCAL_IP]
    if base.namespace is None:
        required.append(DRIVER_ENV_NAMESPACE)
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise PySparkOnK8sError(
            f"Missing environment variables: {', '.join(missing)}. "
            "The task is not running in the driver pod_override; "
            'use the KubernetesExecutor (or executor="KubernetesExecutor" on the task).'
        )


def write_executor_pod_template(executor: SparkExecutorConf, run: RunIdentity, path: str) -> str:
    """Render the executor pod for ``run`` to ``path`` as YAML and return the path."""
    import yaml
    from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    doc = PodGenerator.serialize_pod(executor.render_pod_specification(run))
    with open(path, "w") as f:
        yaml.safe_dump(doc, f, default_flow_style=False)
    return path


def prepare_driver(base: SparkBaseConf, executor: SparkExecutorConf, context: Mapping) -> str:
    """Validate the driver environment and write the executor pod template for this task run."""
    check_driver_env(base)
    run = RunIdentity.from_context(context)
    return write_executor_pod_template(executor, run, base.executor_pod_template_path)
