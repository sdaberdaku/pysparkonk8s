from typing import Any

__version__ = "1.0.0"


def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "airflow-provider-pysparkonk8s",
        "name": "PySparkOnK8s",
        "description": "Run @task.pyspark tasks as client-mode Spark drivers on Kubernetes.",
        "versions": [__version__],
        "task-decorators": [{"name": "pyspark_on_k8s", "class-name": "pysparkonk8s.decorators.pyspark_on_k8s_task"}],
    }
