import os
import tempfile

os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="airflow_home_")
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

import pytest  # noqa: E402


@pytest.fixture
def task():
    try:
        from airflow.sdk import task
    except ImportError:
        from airflow.decorators import task
    return task
