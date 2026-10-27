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

    base: SparkBaseConf
    executor: SparkExecutorConf

    def __call__(self, context: Any) -> None:
        prepare_driver(self.base, self.executor, context)


def pyspark_on_k8s(
    *,
    driver: SparkDriverConf,
    executor: SparkExecutorConf,
    base: SparkBaseConf | None = None,
    extra: Mapping[str, str | None] | None = None,
    executor_name: str | None = None,
) -> dict[str, Any]:
    """Return kwargs for ``@task.pyspark(...).override(**pyspark_on_k8s(...))``."""
    base = base or SparkBaseConf()
    kwargs: dict[str, Any] = {
        "conn_id": None,
        "config_kwargs": build_spark_conf(base, driver, executor, extra),
        "executor_config": {"pod_override": driver.render_pod_specification()},
        "pre_execute": _PrepareDriver(base, executor),
    }
    if executor_name:
        kwargs["executor"] = executor_name
    return kwargs
