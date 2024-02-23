from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import csv

def json_to_csv_data():
    json_file_path = '/Users/thinh/airflow/data/raw_data/weatherdatadetail.json'    
    csv_file_path = '/Users/thinh/airflow/data/clean_data/weatherdatadetail.csv'
    try:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
            
            my_dict = dict( location_id = [], dates = [], temperature = [], feels_like = [], 
                            temp_min = [], temp_max = [], pressure = [], sea_level = [], 
                            grnd_level = [], humidity = [], weather_main = [], weather_description = [], 
                            cloudiness = [], wind_speed = [], wind_direction = [], gust_speed = [], 
                            visibility = [], pop = [], rain_3h = [], retrieval_time =[])
            for i in range(40):
                my_dict['location_id'].append(data['city']['id'])
                my_dict['dates'].append(datetime.fromtimestamp(data['list'][i]['dt']))
                my_dict['temperature'].append(data['list'][i]['main']['temp'])
                my_dict['feels_like'].append(data['list'][i]['main']['feels_like'])
                my_dict['temp_min'].append(data['list'][i]['main']['temp_min'])
                my_dict['temp_max'].append(data['list'][i]['main']['temp_max'])
                my_dict['pressure'].append(data['list'][i]['main']['pressure'])
                my_dict['sea_level'].append(data['list'][i]['main']['sea_level'])
                my_dict['grnd_level'].append(data['list'][i]['main']['grnd_level'])
                my_dict['humidity'].append(data['list'][i]['main']['humidity'])
                my_dict['weather_main'].append(data['list'][i]['weather'][0]['main'])
                my_dict['weather_description'].append(data['list'][i]['weather'][0]['description'])
                my_dict['cloudiness'].append(data['list'][i]['clouds']['all'])
                my_dict['wind_speed'].append(data['list'][i]['wind']['speed'])
                my_dict['wind_direction'].append(data['list'][i]['wind']['deg'])
                my_dict['gust_speed'].append(data['list'][i]['wind']['gust'])
                my_dict['visibility'].append(data['list'][i]['visibility'])
                my_dict['pop'].append(data['list'][i]['pop'])
                rain_3h_value = data['list'][i]['rain']['3h'] if 'rain' in data['list'][i] and '3h' in data['list'][i]['rain'] else 0
                my_dict['rain_3h'].append(rain_3h_value)
                current_date = datetime.now()
                formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
                my_dict['retrieval_time'].append(formatted_date)
            wd = pd.DataFrame(my_dict)

            wd.to_csv(f'{csv_file_path}', index= False)   
            print(f"Chuyển đổi thành công. Dữ liệu dự báo theo giờ đã được lưu vào '{csv_file_path}'.")

    except Exception as e:
        print(f"Loi: {e}")


def postgres_to_local_detail_data():
    hook = PostgresHook(postgres_conn_id = 'postgres_weatherforecast')
    conn = hook.get_conn()
    cursor = conn.cursor()
    current_date = datetime.now()
    current_date = current_date.strftime("%Y-%m-%d")
    current_date
    query = f"""
        WITH maxRetrievalTime AS (
	    SELECT wf.dates, max(wf.retrieval_time) AS max_retrieval_time
	    FROM user_detail ud
	    JOIN user_location ul ON ud.user_id = ul.user_id
	    JOIN locations loc ON ul.location_id = loc.location_id
	    JOIN weatherforecasts wf ON loc.location_id = wf.location_id
	    WHERE 	ud.user_name = 'Thinh' AND 
		    	loc.country = 'Viet Nam' AND 
			    ul.is_activated = 1 AND 
                wf.dates in (
                '{current_date} 01:00:00',
                '{current_date} 04:00:00',
                '{current_date} 07:00:00',
                '{current_date} 10:00:00',
                '{current_date} 13:00:00',
                '{current_date} 16:00:00',
                '{current_date} 19:00:00',
                '{current_date} 22:00:00' 
                )
	    GROUP BY wf.dates
        )
        SELECT wf.temperature, wf.weather_description, wf.humidity, wf.dates, wf.retrieval_time
        FROM weatherforecasts wf
        JOIN maxRetrievalTime mrt ON wf.dates = mrt.dates AND wf.retrieval_time = mrt.max_retrieval_time
        ORDER BY wf.dates ASC, mrt.max_retrieval_time DESC;
    """
    cursor.execute(query=query)
    with open('/Users/thinh/airflow/data/report_data/weatherdatadetail.csv', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()

def postgres_to_local_daily_data():
    hook = PostgresHook(postgres_conn_id = 'postgres_weatherforecast')
    conn = hook.get_conn()
    cursor = conn.cursor()
    day_1 = datetime.now()
    day_2 = day_1 + timedelta(days=1)
    day_3 = day_2 + timedelta(days=1)
    day_4 = day_3 + timedelta(days=1)
    day_5 = day_4 + timedelta(days=1)
    day_1 = day_1.strftime("%Y-%m-%d")
    day_2 = day_2.strftime("%Y-%m-%d")
    day_3 = day_3.strftime("%Y-%m-%d")
    day_4 = day_4.strftime("%Y-%m-%d")
    day_5 = day_5.strftime("%Y-%m-%d")
    query = f"""
            WITH max_retrieval_time AS (
                SELECT wf.dates, max(wf.retrieval_time) AS max_retrieval_time
                FROM user_detail ud
                JOIN user_location ul ON ud.user_id = ul.user_id
                JOIN locations loc ON ul.location_id = loc.location_id
                JOIN weatherforecasts wf ON loc.location_id = wf.location_id
                WHERE 	ud.user_name = 'Thinh' AND 
                        loc.country = 'Viet Nam' AND 
                        ul.is_activated = 1 AND 
                        wf.dates in (
                        '{day_1} 7:00:00',
                        '{day_2} 7:00:00',
                        '{day_3} 7:00:00',
                        '{day_4} 7:00:00',
                        '{day_5} 7:00:00')
                GROUP BY wf.dates
            )
            SELECT wf.temperature, wf.feels_like, wf.pop, wf.humidity, wf.wind_speed, wf.weather_description,
                   wf.cloudiness, wf.visibility, wf.dates, wf.retrieval_time
            FROM weatherforecasts wf
            JOIN max_retrieval_time mrt ON wf.dates = mrt.dates AND wf.retrieval_time = mrt.max_retrieval_time
            ORDER BY wf.dates ASC, mrt.max_retrieval_time DESC;
            """
    cursor.execute(query=query)
    with open('/Users/thinh/airflow/data/report_data/weatherdatadaily.csv', 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()

def create_report():
    pd_data_daily = pd.read_csv('/Users/thinh/airflow/data/report_data/weatherdatadaily.csv')
    pd_data_detail = pd.read_csv('/Users/thinh/airflow/data/report_data/weatherdatadetail.csv')
    
    pd_data_detail['temperature'] = round(pd_data_detail['temperature'] - 273.15).apply(int)
    pd_data_detail['weather_description'] = pd_data_detail['weather_description'].apply(lambda x: x.title())
    
    pd_data_daily['dates'] = pd.to_datetime(pd_data_daily['dates'])
    pd_data_daily['dates'] = pd_data_daily['dates'].dt.strftime('%Y-%m-%d')
    pd_data_daily['temperature'] = round(pd_data_daily['temperature'] -273.15).apply(int)
    pd_data_daily['feels_like'] = round(pd_data_daily['feels_like'] -273.15).apply(int)
    pd_data_daily['weather_description'] = pd_data_daily['weather_description'].apply(lambda x: x.title())
    pd_data_daily['pop'] = round(pd_data_daily['pop']*100).apply(int)
    pd_data_daily['visibility'] = round(pd_data_daily['visibility']/1000).apply(int)
    text = f"""
        <!DOCTYPE html>
        <html lang="vi">
        <head>
        <link href='https://fonts.googleapis.com/css?family=Faustina' rel='stylesheet'>
        <style>
        </style>
        </head>
        <body style="font-family: 'Faustina';">
        <header>
            <h2>Today: {pd_data_daily['dates'][0]}</h2>
        </header>
        <main>
            <table style="margin-bottom: 20px;">
            <thead>
                <tr>
                <th style="width: 250px;"></th>
                <th style="width: 100px;"></th>
                <th style="width: 220px;"></th>
                </tr>
            </thead>
            <tbody>
                <tr>
                <td><h1>{23}°C / {32}°C</h1>
                    All day: {pd_data_daily['weather_description'][0]}
                </td>
                <td></td>
                <td>
                    Possibility of rain {pd_data_daily['pop'][0]}%<br>
                    Humidity: {pd_data_daily['humidity'][0]}%<br>
                    Wind speed: {pd_data_daily['wind_speed'][0]} miles/hour<br>
                    Cloud cover {pd_data_daily['cloudiness'][0]}%<br>
                    Visibility: {pd_data_daily['visibility'][0]}km<br>
                </td>
                </tr>
                <tr>
                <td></td>
                <td></td>
                </tr>
                <tr>
                <td></td>
                <td></td>
                <td></td>
                </tr>
            </tbody>
            </table>
            <section class="now">
            <table>
                <thead>
                <tr>
                    <th style="width: 12.5%;">01:00</th>
                    <th style="width: 12.5%;">04:00</th>
                    <th style="width: 12.5%;">07:00</th>
                    <th style="width: 12.5%;">10:00</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                    <td style="text-align: center; letter-spacing: 1px;">{23}°C</td>
                    <td style="text-align: center; letter-spacing: 1px;">{25}°C</td>
                    <td style="text-align: center; letter-spacing: 1px;">{27}°C</td>
                    <td style="text-align: center; letter-spacing: 1px;">{30}°C</td>
                </tr>
                <tr>
                    <td style="text-align: center;">{pd_data_detail['weather_description'][0]}</td>
                    <td style="text-align: center;">{pd_data_detail['weather_description'][1]}</td>
                    <td style="text-align: center;">{pd_data_detail['weather_description'][2]}</td>
                    <td style="text-align: center;">{pd_data_detail['weather_description'][3]}</td>
                </tr>
                <tr>
                    <td style="text-align: center;">{pd_data_detail['humidity'][0]}%</td>
                    <td style="text-align: center;">{pd_data_detail['humidity'][1]}%</td>
                    <td style="text-align: center;">{pd_data_detail['humidity'][2]}%</td>
                    <td style="text-align: center;">{pd_data_detail['humidity'][3]}%</td>
                </tr>
                </tbody>
            </table>
            <table>
                <thead>
                <tr>
                    <th style="width: 12.5%;">13:00</th>
                    <th style="width: 12.5%;">16:00</th>
                    <th style="width: 12.5%;">19:00</th>
                    <th style="width: 12.5%;">22:00</th>
                </tr>
                </thead>
                <tbody>
                <tr>
                    <td style="text-align: center; letter-spacing: 1px;">{31}°C</td>
                    <td style="text-align: center; letter-spacing: 1px;">{30}°C</td>
                    <td style="text-align: center; letter-spacing: 1px;">{28}°C</td>
                    <td style="text-align: center; letter-spacing: 1px;">{25}°C</td>
                </tr>
                <tr>
                    <td style="text-align: center;">{pd_data_detail['weather_description'][4]}</td>
                    <td style="text-align: center;">{pd_data_detail['weather_description'][5]}</td>
                    <td style="text-align: center;">{pd_data_detail['weather_description'][6]}</td>
                    <td style="text-align: center;">{pd_data_detail['weather_description'][7]}</td>
                </tr>
                <tr>
                    <td style="text-align: center;">{pd_data_detail['humidity'][4]}%</td>
                    <td style="text-align: center;">{pd_data_detail['humidity'][5]}%</td>
                    <td style="text-align: center;">{pd_data_detail['humidity'][6]}%</td>
                    <td style="text-align: center;">{pd_data_detail['humidity'][7]}%</td>
                </tr>
                </tbody>
            </table>
            </section>
            <section class="forecast">
            <h2 style="margin-left: 30px;">The next days</h2>
            <table>
                <thead>
                <tr>
                    <th style="width: 180px;"></th>
                    <th style="width: 180px"></th>
                    <th style="width: 130px;"></th>
                    <th style="width: 150px;"></th>
                </tr>
                </thead>
                <tbody>
                <tr style="height: 30px;">
                    <td style="text-align: center;">{pd_data_daily['dates'][1]}</td>
                </tr>
                <tr style="height: 30px;">
                    <td style="text-align: center">All day: {pd_data_daily['weather_description'][1]}</td>
                    <td style="text-align: center">Temperature: <strong>{23}°C / {33}°C </strong></td>
                    <td style="text-align: center">Humidity: <strong>{pd_data_daily['humidity'][1]}%</strong></td>
                    <td class="humidity" style="text-align: center">Cloud cover <strong>{pd_data_daily['cloudiness'][1]}%</strong></td>
                </tr>
                </tr>
                <tr style="height: 30px;">
                    <td style="text-align: center;">{pd_data_daily['dates'][2]}</td>
                </tr>
                <tr style="height: 30px;">
                    <td style="text-align: center">All day: {pd_data_daily['weather_description'][2]}</td>
                    <td style="text-align: center">Temperature: <strong>{23}°C / {33}°C </strong></td>
                    <td style="text-align: center">Humidity: <strong>{pd_data_daily['humidity'][2]}%</strong></td>
                    <td class="humidity" style="text-align: center">Cloud cover <strong>{pd_data_daily['cloudiness'][2]}%</strong></td>
                </tr>
                <tr style="height: 30px;">
                    <td style="text-align: center;">{pd_data_daily['dates'][3]}</td>
                </tr>
                <tr style="height: 30px;">
                    <td style="text-align: center">All day: {pd_data_daily['weather_description'][3]}</td>
                    <td style="text-align: center">Temperature: <strong>{23}°C / {34}°C </strong></td>
                    <td style="text-align: center">Humidity: <strong>{pd_data_daily['humidity'][3]}%</strong></td>
                    <td class="humidity" style="text-align: center">Cloud cover <strong>{pd_data_daily['cloudiness'][3]}%</strong></td>
                </tr>
                <tr style="height: 30px;">
                    <td style="text-align: center;">{pd_data_daily['dates'][4]}</td>
                </tr>
                <tr style="height: 30px;">
                    <td style="text-align: center">All day: {pd_data_daily['weather_description'][4]}</td>
                    <td style="text-align: center">Temperature: <strong>{23}°C / {34}°C </strong></td>
                    <td style="text-align: center">Humidity: <strong>{pd_data_daily['humidity'][4]}%</strong></td>
                    <td class="humidity" style="text-align: center">Cloud cover <strong>{pd_data_daily['cloudiness'][4]}%</strong></td>
                </tr>
                </tbody>
            </table>
            </section>
        </main>
        </body>
        </html>
    """
    return text

text = create_report()

default_args = {
    'owner': 'thinh',
    'depends_on_past': False,
    'retries': 2,
    'start_date': datetime(2024, 1, 16),
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@daily'
}

with DAG(
    default_args=default_args,
    dag_id='workflow_call_api_weatherforecast',
) as dag:
    # DETAIL
    task_check_weather_api_detail_data = BashOperator(
        task_id = 'check_api_weather_data_detail',
        bash_command= f'airflow tasks test check_api_weather_from_http is_weather_api_detail_ready {datetime.now().strftime("%Y-%m-%d")}'
    )

    task_get_detail_data_from_api = BashOperator(
        task_id = 'get_detail_data',
        bash_command= f'airflow tasks test check_api_weather_from_http get_data_detail {datetime.now().strftime("%Y-%m-%d")}'
    )

    task_save_detail_data = BashOperator(
        task_id = 'save_detail_data',
        bash_command= f'airflow tasks test check_api_weather_from_http save_weather_data_detail {datetime.now().strftime("%Y-%m-%d")}'
    )

    task_convert_json_to_csv_data_detail = PythonOperator(
        task_id = 'json_to_csv_data_detail',
        python_callable=json_to_csv_data
    )

    task_save_detail_data_to_db = PostgresOperator(
        task_id = 'save_detail_data_to_db',
        postgres_conn_id = 'postgres_weatherforecast',
        sql = """
            copy WeatherForecasts(location_id,dates,temperature,feels_like,temp_min,temp_max,pressure,sea_level,
            grnd_level,humidity,weather_main,weather_description,cloudiness,wind_speed,wind_direction,gust_speed,
            visibility,pop,rain_3h,retrieval_time)
            from '/Users/thinh/airflow/data/clean_data/weatherdatadetail.csv'
            delimiter ','
            csv header;
            """
    )

    # QUERY
    task_query_detail_data = PythonOperator(
        task_id = 'query_detail_data',
        python_callable=postgres_to_local_detail_data
    )

    task_query_daily_data = PythonOperator(
        task_id = 'query_daily_data',
        python_callable=postgres_to_local_daily_data
    )
    
    # EMAIL
    task_send_email = EmailOperator(
        task_id = 'send_email',
        to = 'thinhblogdata@gmail.com',
        subject = 'Weather forecast for today',
        html_content = text
    )

    task_check_weather_api_detail_data >> task_get_detail_data_from_api >> task_save_detail_data >> task_convert_json_to_csv_data_detail >> task_save_detail_data_to_db >> [task_query_detail_data , task_query_daily_data] >> task_send_email
