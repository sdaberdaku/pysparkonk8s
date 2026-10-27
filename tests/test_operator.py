from types import SimpleNamespace

import pytest

from pysparkonk8s import PySparkOnK8sOperator, SparkBaseConf, SparkDriverConf, SparkExecutorConf


def test_operator_sets_executor_config_and_conf():
    op = PySparkOnK8sOperator(
        task_id="t",
        python_callable=lambda spark: 1,
        driver_conf=SparkDriverConf(image="d"),
        executor_conf=SparkExecutorConf(image="e"),
        extra={"spark.y": "2"},
        executor_config={"KubernetesExecutor": {"annotations": {"a": "b"}}},
    )
    assert op.executor_config["pod_override"].spec.containers[0].image == "d"
    assert op.executor_config["KubernetesExecutor"] == {"annotations": {"a": "b"}}
    assert op.config_kwargs["spark.y"] == "2"
    assert op.conn_id is None


def test_operator_rejects_user_pod_override():
    with pytest.raises(ValueError, match="pod_override"):
        PySparkOnK8sOperator(
            task_id="t",
            python_callable=lambda spark: 1,
            driver_conf=SparkDriverConf(image="d"),
            executor_conf=SparkExecutorConf(image="e"),
            executor_config={"pod_override": object()},
        )


def test_operator_execute_writes_template_then_runs(tmp_path, monkeypatch, mocker):
    for n, v in {"SPARK_DRIVER_POD_NAME": "p", "SPARK_LOCAL_IP": "ip", "SPARK_DRIVER_NAMESPACE": "ns"}.items():
        monkeypatch.setenv(n, v)
    op = PySparkOnK8sOperator(
        task_id="t",
        python_callable=lambda spark: 1,
        base_conf=SparkBaseConf(executor_pod_template_path=str(tmp_path / "t.yaml")),
        driver_conf=SparkDriverConf(image="d"),
        executor_conf=SparkExecutorConf(image="e"),
    )
    parent = mocker.patch("pysparkonk8s.operators.PySparkOperator.execute", return_value="ok")
    ctx = {"dag": SimpleNamespace(dag_id="d"), "task": op, "run_id": "r", "ti": SimpleNamespace(map_index=-1)}
    assert op.execute(ctx) == "ok"
    assert (tmp_path / "t.yaml").exists()
    parent.assert_called_once()


def test_package_import_does_not_import_airflow_operator():
    import subprocess
    import sys

    code = (
        "import sys, pysparkonk8s; assert 'pysparkonk8s.operators' not in sys.modules; "
        "from pysparkonk8s import PySparkOnK8sOperator"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
