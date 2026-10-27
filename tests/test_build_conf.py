from pysparkonk8s.conf import SparkBaseConf, SparkDriverConf, SparkExecutorConf, build_spark_conf


def test_merge_order_and_unset():
    conf = build_spark_conf(
        SparkBaseConf(),
        SparkDriverConf(image="d"),
        SparkExecutorConf(image="e"),
        extra={
            "spark.dynamicAllocation.enabled": None,
            "spark.sql.shuffle.partitions": "200",
            "spark.master": "k8s://https://other:6443",
        },
    )
    assert "spark.dynamicAllocation.enabled" not in conf
    assert conf["spark.sql.shuffle.partitions"] == "200"
    assert conf["spark.master"] == "k8s://https://other:6443"
    assert conf["spark.driver.memory"] == "1024m"
    assert conf["spark.kubernetes.executor.container.image"] == "e"
    assert all(isinstance(v, str) for v in conf.values())
