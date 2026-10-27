"""``PySparkOnK8sOperator``: ``PySparkOperator`` whose Spark driver and executors run on Kubernetes."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from airflow.providers.apache.spark.operators.spark_pyspark import PySparkOperator

from pysparkonk8s.conf import SparkBaseConf, SparkDriverConf, SparkExecutorConf, build_spark_conf
from pysparkonk8s.template import prepare_driver


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
        if "pod_override" in executor_config:
            raise ValueError("executor_config must not set pod_override: it is built from `driver_conf`")
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
            executor_config={**executor_config, "pod_override": driver_conf.render_pod_specification()},
            **kwargs,
        )

    def execute(self, context: Any) -> Any:
        prepare_driver(self.base_conf, self.executor_conf, context)
        return super().execute(context)
