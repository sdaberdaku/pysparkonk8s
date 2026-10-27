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
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_PySparkOnK8sDecoratedOperator,
        **kwargs,
    )
