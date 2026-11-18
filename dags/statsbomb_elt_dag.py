"""StatsBomb ELT: extract -> PySpark transform -> dbt (star schema + tests).

Configuration comes from environment variables (see .env.example):

LAKE_ROOT       s3://bucket or a local path (default /opt/airflow/data_lake)
SPARK_RUNNER    local (spark-submit inside the Airflow worker) | glue (AWS Glue job)
GLUE_JOB_NAME   Glue job created by Terraform (SPARK_RUNNER=glue)
DBT_TARGET      duckdb (local lake) | athena (S3 lake + Glue Catalog)
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

AIRFLOW_HOME = "/opt/airflow"


def _lake_root() -> str:
    if os.getenv("LAKE_ROOT"):
        return os.environ["LAKE_ROOT"].rstrip("/")
    if os.getenv("S3_BUCKET_NAME"):
        return f"s3://{os.environ['S3_BUCKET_NAME']}"
    return f"{AIRFLOW_HOME}/data_lake"


LAKE_ROOT = _lake_root()
SPARK_RUNNER = os.getenv("SPARK_RUNNER", "local").lower()
DBT_TARGET = os.getenv("DBT_TARGET", "duckdb")

default_args = {
    "owner": "sahil",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="statsbomb_elt",
    description="StatsBomb open data -> S3 lake -> PySpark -> dbt star schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # the Spark job is a full refresh; never overlap runs
    dagrun_timeout=timedelta(hours=3),
    tags=["sports_analytics", "statsbomb", "elt"],
    doc_md=__doc__,
) as dag:

    extract = BashOperator(
        task_id="extract_raw",
        bash_command=f"python {AIRFLOW_HOME}/scripts/extract_statsbomb.py --lake-root '{LAKE_ROOT}'",
        execution_timeout=timedelta(minutes=45),
    )

    if SPARK_RUNNER == "glue":
        from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

        transform = GlueJobOperator(
            task_id="spark_transform",
            job_name=os.environ.get("GLUE_JOB_NAME", "statsbomb-dev-statsbomb-transform"),
            wait_for_completion=True,
            verbose=True,  # stream Glue driver logs into the Airflow task log
            execution_timeout=timedelta(hours=1),
        )
    else:
        # Local Spark talks to S3 through s3a://; plain paths are used as-is.
        spark_lake_root = LAKE_ROOT.replace("s3://", "s3a://", 1)
        transform = BashOperator(
            task_id="spark_transform",
            bash_command=(
                "spark-submit --master 'local[*]' --driver-memory 3g "
                "--jars /opt/spark-jars/hadoop-aws.jar,/opt/spark-jars/aws-java-sdk-bundle.jar "
                "--conf spark.hadoop.fs.s3a.aws.credentials.provider="
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain "
                f"{AIRFLOW_HOME}/spark/jobs/statsbomb_transform.py --lake-root '{spark_lake_root}'"
            ),
            execution_timeout=timedelta(hours=1),
        )

    dbt_env = {
        "DBT_PROFILES_DIR": f"{AIRFLOW_HOME}/dbt",
        "DBT_TARGET": DBT_TARGET,
        "DBT_DUCKDB_PATH": f"{AIRFLOW_HOME}/dbt/statsbomb.duckdb",
        "DBT_PROCESSED_ROOT": f"{LAKE_ROOT}/processed/statsbomb",
    }
    dbt_cmd = f"cd {AIRFLOW_HOME}/dbt && dbt --no-use-colors"

    with TaskGroup(group_id="dbt") as dbt:
        deps = BashOperator(
            task_id="deps",
            bash_command=f"{dbt_cmd} deps",
            env=dbt_env,
            append_env=True,
        )
        freshness = BashOperator(
            task_id="source_freshness",
            bash_command=f"{dbt_cmd} source freshness",
            env=dbt_env,
            append_env=True,
        )
        # `dbt build` runs models, unit tests and data tests in DAG order: a failing
        # test on a staging model stops the marts built on top of it.
        build = BashOperator(
            task_id="build",
            bash_command=f"{dbt_cmd} build",
            env=dbt_env,
            append_env=True,
            execution_timeout=timedelta(hours=1),
        )
        deps >> freshness >> build

    extract >> transform >> dbt
