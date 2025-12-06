from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.Bronze import process_bronze
from src.Silver import process_silver
from src.Gold import process_gold
import os

BASE_PATH = '/opt/airflow/data'
SOURCE_PATH = f'{BASE_PATH}/earthquake_data.csv'
BRONZE_PATH = f'{BASE_PATH}/bronze/earthquake_data.parquet'
SILVER_TABLE = 'silver.silver_data'
GOLD_TABLE = 'gold.gold_data'

user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')
db = os.getenv('POSTGRES_DB')
port = os.getenv('POSTGRES_PORT')
host = 'postgres'
con_str = f"postgresql://{user}:{password}@{host}:{port}/{db}"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG (
    dag_id='earthquake_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    task_bronze = PythonOperator(
        task_id='process_bronze',
        python_callable=process_bronze,
        op_kwargs={
            'source_path': SOURCE_PATH,
            'bronze_path': BRONZE_PATH
        }
    )

    task_silver = PythonOperator(
        task_id='process_silver',
        python_callable=process_silver,
        op_kwargs={
            'bronze_path': BRONZE_PATH,
            'con_str': con_str,
            'silver_table': SILVER_TABLE
        }
    )

    task_gold = PythonOperator(
        task_id='process_gold',
        python_callable=process_gold,
        op_kwargs={
            'silver_table': SILVER_TABLE,
            'con_str': con_str,
            'gold_table': GOLD_TABLE
        }
    )

    task_bronze >> task_silver >> task_gold    