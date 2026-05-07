from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# 1. Define the default settings for this specific pipeline
default_args = {
    'owner': 'sahil',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1), # A date in the past so it can start immediately
    'email_on_failure': False,
    'retries': 1, # If the API fails, try again once
    'retry_delay': timedelta(minutes=5), # Wait 5 minutes before retrying
}

# 2. Instantiate the DAG (The Blueprint)
with DAG(
    'statsbomb_daily_extraction',
    default_args=default_args,
    description='Extracts StatsBomb competition data daily',
    schedule_interval=timedelta(days=1), # Run this once a day
    catchup=False, # Don't try to run it for every day since Jan 1st
    tags=['sports_analytics', 'statsbomb'],
) as dag:

    # 3. Define the Task (The Machine on the factory line)
    # We use a BashOperator to run the exact terminal command you ran manually earlier
    extract_data = BashOperator(
        task_id='run_extraction_script',
        bash_command='python /opt/airflow/scripts/extract_statsbomb.py',
    )

# 4. Define the Transformation Task
    transform_data = BashOperator(
        task_id='run_transformation_script',
        bash_command='python /opt/airflow/scripts/transform_statsbomb.py',
    )

    # 5. Set the Order of Operations (Dependencies)
    # This tells Airflow: Do NOT run transform until extract is 100% successful.
    extract_data >> transform_data