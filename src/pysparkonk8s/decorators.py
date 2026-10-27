"""``@task.pyspark_on_k8s``."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Sequence

from airflow.providers.apache.spark.operators.spark_pyspark import SPARK_CONTEXT_KEYS
from airflow.providers.common.compat.sdk import DecoratedOperator, TaskDecorator, task_decorator_factory

from pysparkonk8s.operators import PySparkOnK8sOperator


class _PySparkOnK8sDecoratedOperator(DecoratedOperator, PySparkOnK8sOperator):
    custom_operator_name = "@task.pyspark_on_k8s"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Sequence | None = None,
        op_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }

        signature = inspect.signature(python_callable)
        parameters = [
            param.replace(default=None) if param.name in SPARK_CONTEXT_KEYS else param
            for param in signature.parameters.values()
        ]
        python_callable.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]

        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def pyspark_on_k8s_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    # Mapped tasks (`.expand`) never run the operator __init__ in the scheduler, so the pod_override
    # must already sit in the decorator kwargs (partial_kwargs) for KubernetesExecutor to see it.
    driver_conf = kwargs.get("driver_conf")
    if driver_conf is not None:
        executor_config = kwargs.get("executor_config") or {}
        kwargs["executor_config"] = {**executor_config, "pod_override": driver_conf.render_pod_specification()}
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_PySparkOnK8sDecoratedOperator,
        **kwargs,
    )
