from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from datetime import datetime

def save_weather_data(ti) -> None:
    api = ti.xcom_pull(task_ids=['get_data_detail'])
    with open('/Users/thinh/airflow/data/raw_data/weatherdatadetail.json', 'w') as file:
        json.dump(api[0], file)

def save_weather_daily_data(ti) -> None:
    api = ti.xcom_pull(task_ids=['get_daily_weatherdata'])
    with open('/Users/thinh/airflow/data/raw_data/weatherdatadaily.json', 'w') as file:
        json.dump(api[0], file)

with DAG(
    dag_id='check_api_weather_from_http',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 16)
) as dag:
    
    task_check_api = HttpSensor(
        task_id='is_weather_api_detail_ready',
        http_conn_id='api_weather',
        endpoint='/data/2.5/forecast?lat=16.1667&lon=107.8333&appid=7beeda38d3ebd3f1963723c976208e8d'
    )

    task_get_api = SimpleHttpOperator(
        task_id='get_data_detail',
        http_conn_id='api_weather',
        endpoint='/data/2.5/forecast?lat=16.1667&lon=107.8333&appid=7beeda38d3ebd3f1963723c976208e8d',
        method='GET',
        response_filter=lambda r: json.loads(r.text)
    )

    task_save_api_data = PythonOperator(
        task_id='save_weather_data_detail',
        python_callable=save_weather_data,
    )

    # get api daily data
    task_check_daily_api = HttpSensor(
        task_id='is_daily_api_ready',
        http_conn_id='api_weather',
        endpoint='/data/2.5/forecast/daily?lat=16.1667&lon=107.8333&cnt=7&appid=7beeda38d3ebd3f1963723c976208e8d'
    )

    task_get_daily_api = SimpleHttpOperator(
        task_id='get_daily_weatherdata',
        http_conn_id='api_weather',
        endpoint='/data/2.5/forecast/daily?lat=16.1667&lon=107.8333&cnt=8&appid=7beeda38d3ebd3f1963723c976208e8d'
    )

    task_save_daily_api = PythonOperator(
        task_id='save_daily_weatherdata',
        python_callable=save_weather_daily_data
    )
