from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.providers.common.sql.operators.sql import MsSqlOperator
# from airflow.contrib.operators.spark_jdbc_operator import
# from airflow.contrib.operators.spark_sql_operator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from api_request import get_data, get_params

endpoint = get_params()[1]

default_args = {
    "owner": 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'email': 'admin@localhost.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG("temperature_data_pipline", start_date=datetime(2023,11,1), schedule='@daily', default_args = default_args, catchup=False) as dag:
    is_forex_rates_available = HttpSensor(
        task_id='is_temperature_available',
        http_conn_id='temperature_api',
        endpoint=endpoint,
        response_check = lambda response: 'rates' in response.text,
        poke_interval=5,
        timeout=20
    )

    is_city_file_available = FileSensor(
        task_id = 'is_city_file_available',
        fs_conn_id = 'city_file',
        filepath = './files/city.json',
        poke_interval=5,
        timeout=20

    )

    download_rate = PythonOperator(
        task_id='download_rates',
        python_callable=get_data
    )

    raw_forex_processing = SparkSubmitOperator(
        task_id='raw_forex_processing',
        application='./scripts/pyspark/transform.py',
        conn_id='raw_spark_conn',
        verbose=False
    )