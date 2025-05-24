from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(project_root)

from tasks.etl import (
    extract_api_task,
    extract_db_task,
    transform_api_task,
    transform_db_task,
    merge_data_task,
    validate_data_task,
    load_model_task,
    send_to_kafka_task

)

default_args = {
    'owner': 'kevinmg',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=200),
}

with DAG(
    dag_id='accidents_etl',
    description='ETL pipeline for Traffic Accidents dataset',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 4, 5),
    catchup=False,
    tags=['accidents', 'ETL'],
) as dag:
    extract_api = PythonOperator(
    task_id="extract_api_data",
    python_callable=extract_api_task,
    dag=dag
)

extract_db = PythonOperator(
    task_id="extract_db_data",
    python_callable=extract_db_task,
    dag=dag
)

transform_api = PythonOperator(
    task_id="transform_api_data",
    python_callable=transform_api_task,
    dag=dag
)

transform_db = PythonOperator(
    task_id="transform_db_data",
    python_callable=transform_db_task,
    dag=dag
)

merge = PythonOperator(
    task_id="merge_data",
    python_callable=merge_data_task,
    dag=dag
)
validate = PythonOperator(
    task_id="validate_data",
    python_callable=validate_data_task,
    provide_context=True,
    dag=dag,
)

load_model = PythonOperator(
    task_id="load_model_data",
    python_callable=load_model_task,
    dag=dag
)

send_kafka = PythonOperator(
    task_id="send_to_kafka",
    python_callable=send_to_kafka_task,
    execution_timeout=timedelta(minutes=60),
    dag=dag
)

end = EmptyOperator(task_id="end")

extract_api >>  transform_api
extract_db >> transform_db
[transform_api, transform_db] >> merge >> validate >> load_model >> send_kafka >> end

