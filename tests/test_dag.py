"""DAG integrity tests. Skipped unless Airflow is installed (CI runs them in a dedicated job)."""
import importlib
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


def load_dag(monkeypatch, **env):
    for key in ("LAKE_ROOT", "S3_BUCKET_NAME", "SPARK_RUNNER", "DBT_TARGET", "GLUE_JOB_NAME"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.syspath_prepend(str(DAGS_DIR))
    sys.modules.pop("statsbomb_elt_dag", None)
    return importlib.import_module("statsbomb_elt_dag").dag


def test_dagbag_imports_cleanly():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert bag.import_errors == {}
    assert "statsbomb_elt" in bag.dags


def test_task_order(monkeypatch):
    dag = load_dag(monkeypatch)
    order = [t.task_id for t in dag.topological_sort()]
    assert order == ["extract_raw", "spark_transform", "dbt.deps", "dbt.source_freshness", "dbt.build"]
    assert dag.max_active_runs == 1


def test_local_runner_uses_s3a_for_s3_lakes(monkeypatch):
    dag = load_dag(monkeypatch, LAKE_ROOT="s3://my-lake", SPARK_RUNNER="local")
    cmd = dag.get_task("spark_transform").bash_command
    assert "spark-submit" in cmd and "--lake-root 's3a://my-lake'" in cmd
    assert "--lake-root 's3://my-lake'" in dag.get_task("extract_raw").bash_command


def test_glue_runner(monkeypatch):
    dag = load_dag(monkeypatch, LAKE_ROOT="s3://my-lake", SPARK_RUNNER="glue", GLUE_JOB_NAME="my-job")
    task = dag.get_task("spark_transform")
    assert type(task).__name__ == "GlueJobOperator" and task.job_name == "my-job"


def test_dbt_points_at_processed_zone(monkeypatch):
    dag = load_dag(monkeypatch, DBT_TARGET="athena")
    env = dag.get_task("dbt.build").env
    assert env["DBT_TARGET"] == "athena"
    assert env["DBT_PROCESSED_ROOT"] == "/opt/airflow/data_lake/processed/statsbomb"
