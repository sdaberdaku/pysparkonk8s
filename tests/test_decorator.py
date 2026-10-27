import pendulum

from pysparkonk8s import SparkDriverConf, SparkExecutorConf


def test_decorator_is_registered_through_providers_manager():
    from airflow.providers_manager import ProvidersManager

    assert "pyspark_on_k8s" in ProvidersManager().taskflow_decorators


def test_task_pyspark_on_k8s(task):
    from airflow.providers.common.compat.sdk import DAG

    @task.pyspark_on_k8s(driver_conf=SparkDriverConf(image="d"), executor_conf=SparkExecutorConf(image="e"))
    def job(spark):
        return spark

    # The decorator keeps raw kwargs until the operator is built inside a DAG.
    with DAG("d", start_date=pendulum.datetime(2025, 1, 1)):
        op = job.override(task_id="j")().operator
    assert op.executor_config["pod_override"].spec.containers[0].image == "d"
    assert op.config_kwargs["spark.kubernetes.executor.container.image"] == "e"


def test_decorator_kwargs_carry_pod_override_for_mapped_tasks():
    from pysparkonk8s import CPU, Memory, SparkDriverConf, SparkExecutorConf
    from pysparkonk8s.decorators import pyspark_on_k8s_task

    @pyspark_on_k8s_task(
        driver_conf=SparkDriverConf(image="d", cores=CPU.cores(1), memory=Memory.mebibytes(512)),
        executor_conf=SparkExecutorConf(image="e", cores=CPU.cores(1), memory=Memory.mebibytes(512), max_executors=1),
    )
    def f(spark=None): ...

    assert f.kwargs["executor_config"]["pod_override"].spec.containers[0].image == "d"
