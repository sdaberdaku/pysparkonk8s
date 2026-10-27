from pysparkonk8s.conf import SparkBaseConf


def test_base_defaults_use_env_substitution():
    conf = SparkBaseConf().render_spark_conf()
    assert conf["spark.master"] == "k8s://https://kubernetes.default.svc:443"
    assert conf["spark.submit.deployMode"] == "client"
    assert conf["spark.kubernetes.driver.pod.name"] == "${env:SPARK_DRIVER_POD_NAME}"
    assert conf["spark.driver.host"] == "${env:SPARK_LOCAL_IP}"
    assert conf["spark.kubernetes.namespace"] == "${env:SPARK_DRIVER_NAMESPACE}"
    assert conf["spark.kubernetes.executor.podTemplateFile"] == "/tmp/pysparkonk8s/executor-pod-template.yaml"
    assert conf["spark.driver.port"] == "7078"
    assert "spark.kubernetes.container.image.pullSecrets" not in conf
    assert not [k for k in conf if "authenticate" in k]


def test_base_explicit_namespace_and_pull_secrets():
    conf = SparkBaseConf(namespace="spark", image_pull_secrets="regcred").render_spark_conf()
    assert conf["spark.kubernetes.namespace"] == "spark"
    assert conf["spark.kubernetes.container.image.pullSecrets"] == "regcred"


def test_base_client_mode_auth_files_when_set():
    conf = SparkBaseConf(ca_cert_file="/ca.crt", oauth_token_file="/token").render_spark_conf()
    assert conf["spark.kubernetes.authenticate.caCertFile"] == "/ca.crt"
    assert conf["spark.kubernetes.authenticate.oauthTokenFile"] == "/token"
    assert not [k for k in conf if "submission" in k]
