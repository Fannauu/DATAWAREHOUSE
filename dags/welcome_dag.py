from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import json
import pandas as pd
import os
import pytz

phnom_penh_tz = pytz.timezone('Asia/Phnom_Penh')

def kelvin_to_celsius(kelvin):
    return round(kelvin - 273.15, 2)

def load_to_minio():
    try: 
        hook = S3Hook(aws_conn_id='minio_conn')
        file_path = "/opt/airflow/dags/weather_data.csv"
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"{file_path} does not exist.")

        hook.load_file(
            filename=file_path,
            key='weather_data.csv',
            bucket_name='weather-data',
            replace=True
        )
        print("File uploaded to MinIO successfully.")
    except Exception as e:
        print(f"Error uploading file to MinIO: {e}")
        raise



def transform_weather_data(ti):
    try:
        weather_data = ti.xcom_pull(task_ids="extract_weather")
        weather_json = json.loads(weather_data)

        city = weather_json["name"]
        description = weather_json["weather"][0]["description"]

        temp_celsius = kelvin_to_celsius(weather_json["main"]["temp"])
        feels_like_celsius = kelvin_to_celsius(weather_json["main"]["feels_like"])
        temp_min_celsius = kelvin_to_celsius(weather_json["main"]["temp_min"])
        temp_max_celsius = kelvin_to_celsius(weather_json["main"]["temp_max"])

        # Other weather data
        pressure = weather_json["main"]["pressure"]
        humidity = weather_json["main"]["humidity"]
        wind_speed = weather_json["wind"]["speed"]


        # Time data
        record_time = datetime.now(phnom_penh_tz)
        sunrise_time = datetime.fromtimestamp(weather_json["sys"]["sunrise"], tz=pytz.UTC).astimezone(phnom_penh_tz)
        sunset_time = datetime.fromtimestamp(weather_json["sys"]["sunset"], tz=pytz.UTC).astimezone(phnom_penh_tz)


        df = pd.DataFrame([{
            "City": city,
            "Description": description,
            "Temperature (°C)": temp_celsius,
            "Feels Like (°C)": feels_like_celsius,
            "Minimum Temp (°C)": temp_min_celsius,
            "Maximum Temp (°C)": temp_max_celsius,
            "Pressure": pressure,
            "Humidity": humidity,
            "Wind Speed": wind_speed,
            "Time of Record": record_time.strftime("%Y-%m-%d %H:%M:%S"),
            "Sunrise (Local Time)": sunrise_time.strftime("%Y-%m-%d %H:%M:%S"),
            "Sunset (Local Time)": sunset_time.strftime("%Y-%m-%d %H:%M:%S")
        }])

        df.to_csv("/opt/airflow/dags/weather_data.csv",
                  index=False, encoding='utf-8-sig')
        print("Weather data transformed & saved as CSV with all required fields.")
        print(f"Data preview:\n{df.to_string()}")
    except Exception as e:
        print(f"Error transforming weather data: {e}")
        raise

def create_bucket_if_not_exists():
    """Create MinIO bucket if it doesn't exist"""
    try:
        hook = S3Hook(aws_conn_id='minio_conn')
        bucket_name = 'weather-data'
        
        # Check if bucket exists, create if not
        if not hook.check_for_bucket(bucket_name):
            hook.create_bucket(bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket {bucket_name} already exists")
            
    except Exception as e:
        print(f"Error creating bucket: {str(e)}")
        raise

# Coordinates for Phnom Penh, Cambodia
PHNOM_PENH_LAT = "11.5564"
PHNOM_PENH_LON = "104.9282"
API_KEY = "68c8458c4ece3fa867626777478d59ad"

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description='Weather ETL pipeline for Phnom Penh',
    schedule="@daily",  # Every day
    catchup=False,
    tags=['weather', 'etl', 'minio']
) as dag:

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="openweather_api",
        endpoint=f"data/2.5/weather?lat={PHNOM_PENH_LAT}&lon={PHNOM_PENH_LON}&appid={API_KEY}",
        timeout=20,
        poke_interval=5,
        mode='poke'
    )

    # Extract weather data from API
    extract_weather = HttpOperator(
        task_id="extract_weather",
        http_conn_id="openweather_api",
        endpoint=f"data/2.5/weather?lat={PHNOM_PENH_LAT}&lon={PHNOM_PENH_LON}&appid={API_KEY}",
        method="GET",
        response_filter=lambda response: response.text,
        log_response=True
    )

    # Transform the weather data
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_weather_data
    )

    # Create bucket if it doesn't exist
    create_bucket = PythonOperator(
        task_id="create_bucket",
        python_callable=create_bucket_if_not_exists
    )

    # Load data to MinIO
    load_data = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio
    )
    is_api_available >> extract_weather >> transform_data >> create_bucket >> load_data

# import requests
# import json
# import pandas as pd
# from datetime import datetime

# # Your coordinates and API key
# lat = "11.5564"
# lon = "104.9282" 
# api_key = "68c8458c4ece3fa867626777478d59ad"

# # Make the API call
# url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
# response = requests.get(url)
# weather_json = response.json()

# # Transform data
# city = weather_json["name"]
# kelvin_temp = weather_json["main"]["temp"]
# celsius_temp = round(kelvin_temp - 273.15, 2)
# humidity = weather_json["main"]["humidity"]
# description = weather_json["weather"][0]["description"]

# print(f"City: {city}")
# print(f"Temperature: {celsius_temp}°C")
# print(f"Humidity: {humidity}%")
# print(f"Description: {description}")