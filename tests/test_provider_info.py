from pysparkonk8s.provider_info import __version__, get_provider_info


def test_provider_info_declares_decorator():
    info = get_provider_info()
    assert info["package-name"] == "airflow-provider-pysparkonk8s"
    assert info["versions"] == [__version__]
    assert info["task-decorators"] == [
        {"name": "pyspark_on_k8s", "class-name": "pysparkonk8s.decorators.pyspark_on_k8s_task"}
    ]
