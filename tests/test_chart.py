import shutil
import subprocess

import pytest
import yaml


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_chart_renders_role_for_service_account():
    out = subprocess.check_output(
        [
            "helm",
            "template",
            "af",
            "charts/pysparkonk8s-rbac",
            "--set",
            "serviceAccountName=airflow-worker",
        ]
    )
    docs = {d["kind"]: d for d in yaml.safe_load_all(out) if d}
    assert {"Role", "RoleBinding"} <= docs.keys()
    assert docs["RoleBinding"]["subjects"][0]["name"] == "airflow-worker"
    resources = {r for rule in docs["Role"]["rules"] for r in rule["resources"]}
    assert {"pods", "pods/log", "configmaps", "services"} <= resources
