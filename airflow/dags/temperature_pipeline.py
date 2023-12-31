from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from api_request import get_data, get_params

endpoint = get_params()[1]

default_args = {
    "owner": 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG("temperature_data_pipline", start_date=datetime(2023,11,1), schedule='@daily', default_args = default_args, catchup=False) as dag:
    is_temp_data_available = HttpSensor(
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

    api_request = PythonOperator(
        task_id='download_data',
        python_callable=get_data
    )

    raw_data_processing = SparkSubmitOperator(
        task_id='raw_data_processing',
        application='./scripts/pyspark/transform_data.py',
        conn_id='raw_spark_conn',
        verbose=False
    )

    data_agg = SparkSubmitOperator(
        task_id='data_agg',
        application='./scripts/pyspark/aggregation.py',
        conn_id='data_agg_conn',
        verbose=False
    )

is_temp_data_available >> is_city_file_available >> api_request >> raw_data_processing >> data_agg