import os
from types import SimpleNamespace

import pytest
import yaml

from pysparkonk8s.conf import RunIdentity, SparkBaseConf, SparkExecutorConf
from pysparkonk8s.errors import PySparkOnK8sError
from pysparkonk8s.template import check_driver_env, prepare_driver, write_executor_pod_template


def ctx(run_id="manual__2026-10-27T10:00:00+00:00", map_index=-1):
    return {
        "dag": SimpleNamespace(dag_id="my dag"),
        "task": SimpleNamespace(task_id="job"),
        "run_id": run_id,
        "ti": SimpleNamespace(map_index=map_index),
    }


def test_run_identity_from_context_uses_safe_label_values():
    from airflow.providers.cncf.kubernetes.pod_generator import make_safe_label_value

    run = RunIdentity.from_context(ctx())
    assert run.run_id == make_safe_label_value("manual__2026-10-27T10:00:00+00:00")
    assert run.dag_id == make_safe_label_value("my dag")
    assert run.map_index is None
    assert RunIdentity.from_context(ctx(map_index=2)).map_index == 2


def test_check_driver_env_lists_missing(monkeypatch):
    for n in ("SPARK_DRIVER_POD_NAME", "SPARK_LOCAL_IP", "SPARK_DRIVER_NAMESPACE"):
        monkeypatch.delenv(n, raising=False)
    with pytest.raises(PySparkOnK8sError, match="SPARK_DRIVER_POD_NAME.*SPARK_LOCAL_IP.*SPARK_DRIVER_NAMESPACE"):
        check_driver_env(SparkBaseConf())
    monkeypatch.setenv("SPARK_DRIVER_POD_NAME", "p")
    monkeypatch.setenv("SPARK_LOCAL_IP", "10.0.0.1")
    check_driver_env(SparkBaseConf(namespace="spark"))  # namespace given: env not needed
    with pytest.raises(PySparkOnK8sError, match="SPARK_DRIVER_NAMESPACE"):
        check_driver_env(SparkBaseConf())


def test_write_executor_pod_template(tmp_path):
    path = write_executor_pod_template(
        SparkExecutorConf(image="e"), RunIdentity("d", "t", "r"), str(tmp_path / "x" / "t.yaml")
    )
    doc = yaml.safe_load(open(path))
    assert doc["kind"] == "Pod"
    assert doc["metadata"]["labels"]["run_id"] == "r"
    assert doc["spec"]["containers"][0]["name"] == "spark-kubernetes-executor"
    assert "podAffinity" in doc["spec"]["affinity"]


def test_prepare_driver_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_DRIVER_POD_NAME", "p")
    monkeypatch.setenv("SPARK_LOCAL_IP", "ip")
    monkeypatch.setenv("SPARK_DRIVER_NAMESPACE", "ns")
    base = SparkBaseConf(executor_pod_template_path=str(tmp_path / "t.yaml"))
    assert prepare_driver(base, SparkExecutorConf(image="e"), ctx()) == str(tmp_path / "t.yaml")
    assert os.path.exists(tmp_path / "t.yaml")
