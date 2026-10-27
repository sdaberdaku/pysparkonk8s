import pickle
from types import SimpleNamespace

from pysparkonk8s import SparkBaseConf, SparkDriverConf, SparkExecutorConf, pyspark_on_k8s


def test_helper_returns_override_kwargs():
    kw = pyspark_on_k8s(
        driver=SparkDriverConf(image="d"),
        executor=SparkExecutorConf(image="e"),
        extra={"spark.x": "1"},
        executor_name="KubernetesExecutor",
    )
    assert kw["conn_id"] is None
    assert kw["executor"] == "KubernetesExecutor"
    assert kw["config_kwargs"]["spark.x"] == "1"
    assert kw["config_kwargs"]["spark.master"].startswith("k8s://")
    assert kw["executor_config"]["pod_override"].spec.containers[0].name == "base"
    assert callable(kw["pre_execute"])
    assert "executor" not in pyspark_on_k8s(driver=SparkDriverConf(image="d"), executor=SparkExecutorConf(image="e"))


def test_helper_applies_to_stock_decorator(task):
    @task.pyspark
    def job(spark=None):
        return 1

    op = job.override(
        task_id="j", **pyspark_on_k8s(driver=SparkDriverConf(image="d"), executor=SparkExecutorConf(image="e"))
    )
    assert op.kwargs["executor_config"]["pod_override"].metadata.labels["pysparkonk8s/component"] == "driver"
    assert op.kwargs["config_kwargs"]["spark.submit.deployMode"] == "client"


def test_hook_writes_template(tmp_path, monkeypatch):
    for n, v in {"SPARK_DRIVER_POD_NAME": "p", "SPARK_LOCAL_IP": "ip", "SPARK_DRIVER_NAMESPACE": "ns"}.items():
        monkeypatch.setenv(n, v)
    kw = pyspark_on_k8s(
        base=SparkBaseConf(executor_pod_template_path=str(tmp_path / "t.yaml")),
        driver=SparkDriverConf(image="d"),
        executor=SparkExecutorConf(image="e"),
    )
    hook = pickle.loads(pickle.dumps(kw["pre_execute"]))
    ctx = {
        "dag": SimpleNamespace(dag_id="d"),
        "task": SimpleNamespace(task_id="t"),
        "run_id": "r",
        "ti": SimpleNamespace(map_index=-1),
    }
    hook(context=ctx)
    assert (tmp_path / "t.yaml").exists()
