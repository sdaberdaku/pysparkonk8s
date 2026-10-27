"""Run PySpark tasks on Kubernetes from Airflow."""

from pysparkonk8s.conf import (
    FOLLOW_DRIVER,
    SparkBaseConf,
    SparkDriverConf,
    SparkExecutorConf,
    build_spark_conf,
    one_per_node,
    same_zone_as,
)
from pysparkonk8s.errors import PySparkOnK8sError
from pysparkonk8s.helper import pyspark_on_k8s
from pysparkonk8s.provider_info import __version__
from pysparkonk8s.resources import CPU, Memory

__all__ = [
    "CPU",
    "FOLLOW_DRIVER",
    "Memory",
    "PySparkOnK8sError",
    "PySparkOnK8sOperator",
    "SparkBaseConf",
    "SparkDriverConf",
    "SparkExecutorConf",
    "__version__",
    "build_spark_conf",
    "one_per_node",
    "pyspark_on_k8s",
    "same_zone_as",
]


def __getattr__(name: str):
    # Lazy: keeps `import pysparkonk8s` free of Airflow.
    if name == "PySparkOnK8sOperator":
        from pysparkonk8s.operators import PySparkOnK8sOperator

        return PySparkOnK8sOperator
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
