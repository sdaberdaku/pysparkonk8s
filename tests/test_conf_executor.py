import pytest

from pysparkonk8s.conf import (
    COMPONENT_LABEL,
    RunIdentity,
    SparkDriverConf,
    SparkExecutorConf,
    one_per_node,
    same_zone_as,
)
from pysparkonk8s.resources import CPU, Memory

IMG = "spark:4.2.0-python3"
RUN = RunIdentity(dag_id="d", task_id="t", run_id="manual-2026", map_index=None)


def test_run_identity_labels():
    assert RUN.labels() == {"dag_id": "d", "task_id": "t", "run_id": "manual-2026"}
    assert RunIdentity("d", "t", "r", 3).labels()["map_index"] == "3"


def test_executor_conf_dynamic_allocation_defaults():
    conf = SparkExecutorConf(image=IMG, cores=CPU.cores(4), memory=Memory.gibibytes(8)).render_spark_conf()
    assert conf["spark.executor.cores"] == "4"
    assert conf["spark.executor.memory"] == "8192m"
    assert conf["spark.executor.memoryOverhead"] == "820m"
    assert conf["spark.kubernetes.executor.request.cores"] == "4"
    assert conf["spark.dynamicAllocation.enabled"] == "true"
    assert conf["spark.dynamicAllocation.shuffleTracking.enabled"] == "true"
    assert conf["spark.dynamicAllocation.minExecutors"] == "0"
    assert conf["spark.dynamicAllocation.maxExecutors"] == "10"
    assert conf["spark.kubernetes.executor.container.image"] == IMG
    assert conf["spark.kubernetes.executor.podTemplateContainerName"] == "spark-kubernetes-executor"
    assert "spark.executor.instances" not in conf


def test_executor_conf_fixed_instances_and_pyspark_memory():
    e = SparkExecutorConf(
        image=IMG,
        memory=Memory.gibibytes(1),
        memory_overhead=Memory.mebibytes(512),
        pyspark_memory=Memory.mebibytes(512),
        dynamic_allocation=False,
        instances=3,
    )
    conf = e.render_spark_conf()
    assert conf["spark.executor.instances"] == "3"
    assert conf["spark.executor.pyspark.memory"] == "512m"
    assert "spark.dynamicAllocation.maxExecutors" not in conf
    assert e.render_pod_specification(RUN).spec.containers[0].resources.requests["memory"] == "2048Mi"


def test_executor_template_carries_run_labels_and_follow_driver_affinity():
    pod = SparkExecutorConf(image=IMG).render_pod_specification(RUN)
    assert pod.metadata.labels == {COMPONENT_LABEL: "executor", **RUN.labels()}
    terms = pod.spec.affinity.pod_affinity.preferred_during_scheduling_ignored_during_execution
    assert [t.weight for t in terms] == [100, 75, 50, 25]
    assert [t.pod_affinity_term.topology_key for t in terms] == [
        "kubernetes.io/hostname",
        "kubernetes.io/hostname",
        "topology.kubernetes.io/zone",
        "topology.kubernetes.io/zone",
    ]
    assert terms[0].pod_affinity_term.label_selector.match_labels == {**RUN.labels(), COMPONENT_LABEL: "driver"}
    assert terms[1].pod_affinity_term.label_selector.match_labels == {**RUN.labels(), COMPONENT_LABEL: "executor"}


def test_concurrent_runs_select_different_drivers():
    e = SparkExecutorConf(image=IMG)
    a = e.render_pod_specification(RunIdentity("d", "t", "run-a"))
    b = e.render_pod_specification(RunIdentity("d", "t", "run-b", map_index=1))

    def sel(p):
        return p.spec.affinity.pod_affinity.preferred_during_scheduling_ignored_during_execution[
            0
        ].pod_affinity_term.label_selector.match_labels

    assert sel(a)["run_id"] == "run-a" and "map_index" not in sel(a)
    assert sel(b)["run_id"] == "run-b" and sel(b)["map_index"] == "1"


def test_executor_requires_run():
    with pytest.raises(ValueError):
        SparkExecutorConf(image=IMG).render_pod_specification(None)


def test_affinity_none_and_explicit():
    assert SparkExecutorConf(image=IMG, affinity=None).render_pod_specification(RUN).spec.affinity is None
    from kubernetes.client import models as k8s

    custom = k8s.V1Affinity()
    assert SparkExecutorConf(image=IMG, affinity=custom).render_pod_specification(RUN).spec.affinity is custom


def test_placement_rules():
    rule = same_zone_as("eu-west-1a") & one_per_node()
    aff = SparkExecutorConf(image=IMG, affinity=rule).render_pod_specification(RUN).spec.affinity
    req = aff.node_affinity.required_during_scheduling_ignored_during_execution.node_selector_terms[
        0
    ].match_expressions[0]
    assert (req.key, req.operator, req.values) == ("topology.kubernetes.io/zone", "In", ["eu-west-1a"])
    anti = aff.pod_anti_affinity.required_during_scheduling_ignored_during_execution[0]
    assert anti.topology_key == "kubernetes.io/hostname"
    assert anti.label_selector.match_labels == RUN.labels()
    assert anti.label_selector.match_expressions[0].values == ["driver", "executor"]


def test_one_per_node_cluster_wide_has_no_run_labels():
    aff = SparkExecutorConf(image=IMG, affinity=one_per_node(per_run=False)).render_pod_specification(RUN).spec.affinity
    assert (
        aff.pod_anti_affinity.required_during_scheduling_ignored_during_execution[0].label_selector.match_labels is None
    )


def test_driver_accepts_zone_rule_but_not_per_run_rule():
    assert SparkDriverConf(image=IMG, affinity=same_zone_as("z")).render_pod_specification().spec.affinity.node_affinity
    with pytest.raises(ValueError, match="executors only"):
        SparkDriverConf(image=IMG, affinity=one_per_node()).render_pod_specification()
    assert (
        SparkDriverConf(image=IMG, affinity=one_per_node(per_run=False))
        .render_pod_specification()
        .spec.affinity.pod_anti_affinity
    )


def test_executor_limit_cores_renders_the_clamped_value():
    conf = SparkExecutorConf(image=IMG, cores=CPU.cores(2), limit_cpu=CPU.cores(1)).render_spark_conf()
    assert conf["spark.kubernetes.executor.limit.cores"] == "2"


def test_fractional_cores_are_rejected_with_a_pointer_to_request_cpu():
    with pytest.raises(ValueError, match="request_cpu"):
        SparkExecutorConf(image=IMG, cores=CPU.milli_cores(250))
    SparkExecutorConf(image=IMG, cores=CPU.cores(1), request_cpu=CPU.milli_cores(250))
    SparkExecutorConf(image=IMG, cores=None)


def test_component_label_cannot_be_overridden():
    pod = SparkExecutorConf(image=IMG, pod_labels={"pysparkonk8s/component": "x"}).render_pod_specification(
        RunIdentity("d", "t", "r")
    )
    assert pod.metadata.labels["pysparkonk8s/component"] == "executor"
