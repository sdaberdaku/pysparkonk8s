import os
from datetime import datetime, timezone

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

from pysparkonk8s import CPU, Memory, SparkDriverConf, SparkExecutorConf, pyspark_on_k8s

DRIVER_IMAGE = os.environ.get("DRIVER_IMAGE", "pysparkonk8s-driver:e2e")
EXECUTOR_IMAGE = os.environ.get("EXECUTOR_IMAGE", "spark:4.2.0-python3")

# 250m CPU requests (and max_active_tis_per_dag=1 in the mapped DAG) let the e2e fit a 2 vCPU CI runner.
driver_conf = SparkDriverConf(
    image=DRIVER_IMAGE, cores=CPU.cores(1), request_cpu=CPU.milli_cores(250), memory=Memory.mebibytes(1024)
)
executor_conf = SparkExecutorConf(
    image=EXECUTOR_IMAGE,
    cores=CPU.cores(1),
    request_cpu=CPU.milli_cores(250),
    memory=Memory.mebibytes(1024),
    max_executors=2,
)


@dag(
    dag_id="pysparkonk8s_helper",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["pysparkonk8s"],
)
def pysparkonk8s_helper():
    @task.pyspark
    def sum_ids(spark=None):
        return spark.range(1000).selectExpr("sum(id)").collect()[0][0]

    sum_ids.override(**pyspark_on_k8s(driver_conf=driver_conf, executor_conf=executor_conf))()


pysparkonk8s_helper()
