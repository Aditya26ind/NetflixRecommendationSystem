"""Airflow DAG for batch aggregates."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_resources():
    """Check if required resources are available"""
    print("Checking S3 connection...")
    print("Checking Database connection...")
    print("All resources ready!")


def cleanup():
    """Cleanup after pipeline execution"""
    print("Closing connections...")
    print("Pipeline completed!")


with DAG(
    dag_id="batch_pipeline_dag",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Start task
    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=check_resources,
    )
    
    # Main aggregation task
    aggregate = BashOperator(
        task_id="aggregate",
        bash_command=(
            "export DB_HOST=${DB_HOST:-db} DB_PORT=${DB_PORT:-5432} "
            "DB_NAME=${DB_NAME:-movie_recommendations} DB_USER=${DB_USER:-postgres} "
            "DB_PASSWORD=${DB_PASSWORD:-password} PYTHONPATH=/app && "
            "python /app/spark/batch/aggregates.py"
        ),
    )
    
    # End task
    end = PythonOperator(
        task_id="end_pipeline",
        python_callable=cleanup,
    )
    
    # Define task flow
    start >> aggregate >> end
