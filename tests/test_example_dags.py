from pathlib import Path


def test_example_dags_import_cleanly():
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:  # Airflow 2
        from airflow.models import DagBag

    folder = Path(__file__).resolve().parents[1] / "examples" / "dags"
    bag = DagBag(dag_folder=str(folder))
    assert bag.import_errors == {}
    assert set(bag.dag_ids) == {"pysparkonk8s_helper", "pysparkonk8s_decorator", "pysparkonk8s_mapped"}
