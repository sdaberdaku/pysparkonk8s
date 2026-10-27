import pytest

from pysparkonk8s.conf import COMPONENT_LABEL, SparkDriverConf
from pysparkonk8s.resources import CPU, Memory

IMG = "example.com/airflow-spark:1"


def test_image_required():
    with pytest.raises(ValueError, match="image"):
        SparkDriverConf(image="")


def test_driver_memory_request_defaults_to_heap_plus_40_percent():
    d = SparkDriverConf(image=IMG, cores=CPU.cores(2), memory=Memory.gibibytes(10))
    pod = d.render_pod_specification()
    res = pod.spec.containers[0].resources
    assert res.requests == {"cpu": "2", "memory": "14336Mi"}
    assert res.limits == {"memory": "14336Mi"}
    conf = d.render_spark_conf()
    assert conf["spark.driver.memory"] == "10240m"
    assert conf["spark.driver.memoryOverhead"] == "4096m"
    assert conf["spark.driver.cores"] == "2"


def test_overhead_floor_384mi():
    d = SparkDriverConf(image=IMG, memory=Memory.mebibytes(512))
    assert d.render_spark_conf()["spark.driver.memoryOverhead"] == "384m"


def test_explicit_overhead_and_cpu_request_win():
    d = SparkDriverConf(
        image=IMG,
        cores=CPU.cores(4),
        memory=Memory.gibibytes(4),
        memory_overhead=Memory.gibibytes(1),
        request_cpu=CPU.milli_cores(3500),
        limit_cpu=CPU.cores(4),
    )
    res = d.render_pod_specification().spec.containers[0].resources
    assert res.requests == {"cpu": "3500m", "memory": "5120Mi"}
    assert res.limits == {"cpu": "4", "memory": "5120Mi"}


def test_limit_below_request_is_clamped_up():
    d = SparkDriverConf(
        image=IMG,
        memory=Memory.gibibytes(4),
        limit_memory=Memory.gibibytes(1),
        request_cpu=CPU.cores(2),
        limit_cpu=CPU.cores(1),
    )
    res = d.render_pod_specification().spec.containers[0].resources
    assert res.limits["memory"] == res.requests["memory"]
    assert res.limits["cpu"] == "2"


def test_driver_pod_has_component_label_and_downward_env():
    pod = SparkDriverConf(image=IMG, pod_labels={"team": "data"}).render_pod_specification()
    assert pod.metadata.labels == {COMPONENT_LABEL: "driver", "team": "data"}
    c = pod.spec.containers[0]
    assert c.name == "base"
    by_name = {e.name: e.value_from.field_ref.field_path for e in c.env}
    assert by_name == {
        "SPARK_DRIVER_POD_NAME": "metadata.name",
        "SPARK_LOCAL_IP": "status.podIP",
        "SPARK_DRIVER_NAMESPACE": "metadata.namespace",
    }


def test_none_fields_are_omitted():
    d = SparkDriverConf(image=IMG, cores=None, memory=None, extra_java_options=None)
    assert d.render_spark_conf() == {}
    res = d.render_pod_specification().spec.containers[0].resources
    assert res.requests is None and res.limits is None
