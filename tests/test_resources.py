import pytest

from pysparkonk8s.resources import CPU, Memory


@pytest.mark.parametrize("spec,milli", [("1500m", 1500), ("1.5", 1500), ("2", 2000), (3, 3000)])
def test_cpu_from_k8s_spec(spec, milli):
    assert CPU.from_k8s_spec(spec) == CPU.milli_cores(milli)


def test_cpu_renders_both_dialects():
    assert CPU.milli_cores(1500).to_k8s_spec() == "1500m"
    assert CPU.cores(2).to_k8s_spec() == "2"
    assert CPU.milli_cores(1500).to_spark_spec() == "1.5"
    assert CPU.cores(2).to_spark_spec() == "2"


def test_cpu_arithmetic_and_order():
    assert CPU.cores(1) + CPU.milli_cores(500) == CPU.milli_cores(1500)
    assert CPU.cores(2) * 1.5 == CPU.cores(3)
    assert CPU.cores(1) < CPU.cores(2)


@pytest.mark.parametrize("spec,rendered", [("512Mi", "512Mi"), ("2Gi", "2048Mi"), ("1G", "954Mi"), ("1048576", "1Mi")])
def test_memory_from_k8s_spec(spec, rendered):
    assert Memory.from_k8s_spec(spec).to_k8s_spec() == rendered


def test_memory_renders_both_dialects():
    assert Memory.gibibytes(2).to_k8s_spec() == "2048Mi"
    assert Memory.gibibytes(2).to_spark_spec() == "2048m"
    assert Memory.from_spark_spec("1g") == Memory.mebibytes(1024)


def test_memory_arithmetic_rounds_up():
    assert (Memory.mebibytes(1000) * 1.4).to_k8s_spec() == "1400Mi"
    assert (Memory.mebibytes(1) * 0.3).to_k8s_spec() == "1Mi"
    assert Memory.gibibytes(1) + Memory.mebibytes(1) == Memory.mebibytes(1025)


@pytest.mark.parametrize("bad", ["-1", "abc", "1x"])
def test_memory_rejects_garbage(bad):
    with pytest.raises(ValueError):
        Memory.from_k8s_spec(bad)


@pytest.mark.parametrize(
    "spec,expected",
    [("4G", Memory.gibibytes(4)), ("512M", Memory.mebibytes(512)), ("4gb", Memory.gibibytes(4)), ("1b", Memory(1))],
)
def test_memory_from_spark_spec_is_case_and_suffix_tolerant(spec, expected):
    assert Memory.from_spark_spec(spec) == expected


def test_memory_from_k8s_spec_accepts_int_bytes():
    assert Memory.from_k8s_spec(1048576) == Memory.mebibytes(1)
