"""``pyspark_on_k8s``: override kwargs that run the stock ``@task.pyspark`` on Kubernetes."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pysparkonk8s.conf import SparkBaseConf, SparkDriverConf, SparkExecutorConf, build_spark_conf
from pysparkonk8s.template import prepare_driver


@dataclass(frozen=True)
class _PrepareDriver:
    """Picklable ``pre_execute`` hook: writes the executor pod template in the driver pod."""

    base_conf: SparkBaseConf
    executor_conf: SparkExecutorConf

    def __call__(self, context: Any) -> None:
        prepare_driver(self.base_conf, self.executor_conf, context)


def pyspark_on_k8s(
    *,
    driver_conf: SparkDriverConf,
    executor_conf: SparkExecutorConf,
    base_conf: SparkBaseConf | None = None,
    extra: Mapping[str, str | None] | None = None,
    executor_name: str | None = None,
) -> dict[str, Any]:
    """Return kwargs for ``@task.pyspark(...).override(**pyspark_on_k8s(...))``."""
    base_conf = base_conf if base_conf is not None else SparkBaseConf()
    kwargs: dict[str, Any] = {
        "conn_id": None,
        "config_kwargs": build_spark_conf(base_conf, driver_conf, executor_conf, extra),
        "executor_config": {"pod_override": driver_conf.render_pod_specification()},
        "pre_execute": _PrepareDriver(base_conf, executor_conf),
    }
    if executor_name is not None:
        kwargs["executor"] = executor_name
    return kwargs
