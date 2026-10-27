"""``PySparkOnK8sOperator``: ``PySparkOperator`` whose Spark driver and executors run on Kubernetes."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from airflow.providers.apache.spark.operators.spark_pyspark import PySparkOperator
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator

from pysparkonk8s.conf import (
    COMPONENT_LABEL,
    DRIVER_COMPONENT,
    SparkBaseConf,
    SparkDriverConf,
    SparkExecutorConf,
    build_spark_conf,
)
from pysparkonk8s.errors import PySparkOnK8sError
from pysparkonk8s.template import prepare_driver


def _is_driver_pod(pod: Any) -> bool:
    labels = getattr(getattr(pod, "metadata", None), "labels", None) or {}
    return labels.get(COMPONENT_LABEL) == DRIVER_COMPONENT


def check_pod_override(executor_config: Mapping) -> None:
    """Reject a ``pod_override`` that is not the one rendered from ``driver_conf`` (it carries our component label)."""
    incoming = executor_config.get("pod_override")
    if incoming is not None and not _is_driver_pod(incoming):
        raise ValueError("executor_config must not set pod_override: it is built from `driver_conf`")


class PySparkOnK8sOperator(PySparkOperator):
    """Run ``python_callable(spark)`` in a driver pod that starts its executors in-cluster."""

    ui_color = "#028A0F"

    def __init__(
        self,
        *,
        python_callable: Callable,
        driver_conf: SparkDriverConf,
        executor_conf: SparkExecutorConf,
        base_conf: SparkBaseConf | None = None,
        extra: Mapping[str, str | None] | None = None,
        op_args: Sequence | None = None,
        op_kwargs: dict | None = None,
        **kwargs: Any,
    ) -> None:
        executor_config = kwargs.pop("executor_config", None) or {}
        # The decorator pre-fills a pod_override it rendered (mapped tasks need it before the operator exists).
        # It carries our component label and is replaced by a fresh render; any other pod_override is the user's.
        check_pod_override(executor_config)
        # Kept to detect a mapped task whose pod was rendered from a different driver_conf (see execute()).
        self._launched_pod_override = executor_config.get("pod_override")
        pod_override = driver_conf.render_pod_specification()
        if "conn_id" in kwargs:
            raise ValueError("conn_id is not supported: the driver runs in the cluster")
        self.base_conf = base_conf if base_conf is not None else SparkBaseConf()
        self.driver_conf = driver_conf
        self.executor_conf = executor_conf
        self.extra = extra
        super().__init__(
            python_callable=python_callable,
            conn_id=None,
            config_kwargs=build_spark_conf(self.base_conf, driver_conf, executor_conf, extra),
            op_args=op_args,
            op_kwargs=op_kwargs,
            executor_config={**executor_config, "pod_override": pod_override},
            **kwargs,
        )

    def execute(self, context: Any) -> Any:
        launched = self._launched_pod_override
        if launched is not None and context["ti"].map_index >= 0:
            fresh = self.driver_conf.render_pod_specification()
            if PodGenerator.serialize_pod(launched) != PodGenerator.serialize_pod(fresh):
                raise PySparkOnK8sError(
                    "The driver pod was rendered from a different driver_conf than this task uses. "
                    "For mapped tasks, pass driver_conf to @task.pyspark_on_k8s(...) itself; "
                    ".override(driver_conf=...) cannot change the pod of a mapped task."
                )
        prepare_driver(self.base_conf, self.executor_conf, context)
        return super().execute(context)
