from src.extract.extract import (extract_api_task as extract_api_func, extract_db_task as extract_db_func )
from src.transform.transform import (transform_api_data as transform_api_func, transform_db_data as transform_db_func,
                                     merge_data as merge_func)
from src.load.load_model import load_model_data as load_model_func
from src.kafka.producer import send_to_kafka_task as send_to_kafka_func
from airflow.operators.python import PythonOperator
from src.validate.validate import validate_data_with_gx


def extract_api_task(**kwargs):
    extract_api_func(**kwargs)

def extract_db_task(**kwargs):
    extract_db_func(**kwargs)

def transform_api_task(**kwargs):
    transform_api_func(**kwargs)

def transform_db_task(**kwargs):
    transform_db_func(**kwargs)

def merge_data_task(**kwargs):
    merge_func(**kwargs)

def load_model_task(**kwargs):
    load_model_func(**kwargs)

def send_to_kafka_task(**kwargs):
    send_to_kafka_func(**kwargs)

def validate_data_task(**kwargs):
    validate_data_with_gx(**kwargs)