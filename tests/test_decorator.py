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


def test_mapped_task_carries_pod_override(task):
    from airflow.providers.common.compat.sdk import DAG

    @task.pyspark_on_k8s(driver_conf=SparkDriverConf(image="d"), executor_conf=SparkExecutorConf(image="e"))
    def job(n, spark):
        return n

    with DAG("m", start_date=pendulum.datetime(2025, 1, 1)):
        mapped = job.expand(n=[1, 2]).operator
    assert mapped.partial_kwargs["executor_config"]["pod_override"].spec.containers[0].image == "d"


def test_override_driver_conf_replaces_prefilled_pod_override(task):
    from airflow.providers.common.compat.sdk import DAG

    @task.pyspark_on_k8s(driver_conf=SparkDriverConf(image="d"), executor_conf=SparkExecutorConf(image="e"))
    def job(spark):
        return spark

    with DAG("o", start_date=pendulum.datetime(2025, 1, 1)):
        op = job.override(driver_conf=SparkDriverConf(image="other"))().operator
    assert op.executor_config["pod_override"].spec.containers[0].image == "other"
