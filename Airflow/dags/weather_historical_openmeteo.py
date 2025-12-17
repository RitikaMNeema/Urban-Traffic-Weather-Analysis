from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Variable.get("dbt_project_dir", default_var="/opt/airflow/dbt")
api_url = Variable.get("weather_api_url_historical", default_var="https://archive-api.open-meteo.com/v1/archive")
user_db = Variable.get("USER_DB")

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

TARGET_STATE = "CA"
TARGET_STATE_FULL = "California"
DAYS_BACK = 5

CALIFORNIA_CITIES = [
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437, "region": "Southern California", "county": "Los Angeles"},
    {"name": "San Diego", "lat": 32.7157, "lon": -117.1611, "region": "Southern California", "county": "San Diego"},
    {"name": "San Jose", "lat": 37.3382, "lon": -121.8863, "region": "Bay Area", "county": "Santa Clara"},
    {"name": "San Francisco", "lat": 37.7749, "lon": -122.4194, "region": "Bay Area", "county": "San Francisco"},
    {"name": "Fresno", "lat": 36.7378, "lon": -119.7871, "region": "Central Valley", "county": "Fresno"},
    {"name": "Sacramento", "lat": 38.5816, "lon": -121.4944, "region": "Central Valley", "county": "Sacramento"},
    {"name": "Long Beach", "lat": 33.7701, "lon": -118.1937, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Oakland", "lat": 37.8044, "lon": -122.2712, "region": "Bay Area", "county": "Alameda"},
    {"name": "Bakersfield", "lat": 35.3733, "lon": -119.0187, "region": "Central Valley", "county": "Kern"},
    {"name": "Anaheim", "lat": 33.8366, "lon": -117.9143, "region": "Southern California", "county": "Orange"},
    
    # SOUTHERN CALIFORNIA
    {"name": "Santa Ana", "lat": 33.7455, "lon": -117.8677, "region": "Southern California", "county": "Orange"},
    {"name": "Riverside", "lat": 33.9806, "lon": -117.3755, "region": "Southern California", "county": "Riverside"},
    {"name": "Irvine", "lat": 33.6846, "lon": -117.8265, "region": "Southern California", "county": "Orange"},
    {"name": "Chula Vista", "lat": 32.6401, "lon": -117.0842, "region": "Southern California", "county": "San Diego"},
    {"name": "San Bernardino", "lat": 34.1083, "lon": -117.2898, "region": "Southern California", "county": "San Bernardino"},
    {"name": "Fontana", "lat": 34.0922, "lon": -117.4350, "region": "Southern California", "county": "San Bernardino"},
    {"name": "Oxnard", "lat": 34.1975, "lon": -119.1771, "region": "Southern California", "county": "Ventura"},
    {"name": "Moreno Valley", "lat": 33.9425, "lon": -117.2297, "region": "Southern California", "county": "Riverside"},
    {"name": "Huntington Beach", "lat": 33.6603, "lon": -117.9992, "region": "Southern California", "county": "Orange"},
    {"name": "Glendale", "lat": 34.1425, "lon": -118.2551, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Santa Clarita", "lat": 34.3917, "lon": -118.5426, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Oceanside", "lat": 33.1959, "lon": -117.3795, "region": "Southern California", "county": "San Diego"},
    {"name": "Corona", "lat": 33.8753, "lon": -117.5664, "region": "Southern California", "county": "Riverside"},
    {"name": "Palmdale", "lat": 34.5794, "lon": -118.1165, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Pasadena", "lat": 34.1478, "lon": -118.1445, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Torrance", "lat": 33.8358, "lon": -118.3406, "region": "Southern California", "county": "Los Angeles"},
    {"name": "Escondido", "lat": 33.1192, "lon": -117.0864, "region": "Southern California", "county": "San Diego"},
    
    # BAY AREA
    {"name": "Fremont", "lat": 37.5485, "lon": -121.9886, "region": "Bay Area", "county": "Alameda"},
    {"name": "Hayward", "lat": 37.6688, "lon": -122.0808, "region": "Bay Area", "county": "Alameda"},
    {"name": "Sunnyvale", "lat": 37.3688, "lon": -122.0363, "region": "Bay Area", "county": "Santa Clara"},
    {"name": "Santa Clara", "lat": 37.3541, "lon": -121.9552, "region": "Bay Area", "county": "Santa Clara"},
    {"name": "Berkeley", "lat": 37.8715, "lon": -122.2730, "region": "Bay Area", "county": "Alameda"},
    {"name": "Concord", "lat": 37.9780, "lon": -122.0311, "region": "Bay Area", "county": "Contra Costa"},
    {"name": "Vallejo", "lat": 38.1041, "lon": -122.2566, "region": "Bay Area", "county": "Solano"},
    {"name": "Santa Rosa", "lat": 38.4404, "lon": -122.7141, "region": "Bay Area", "county": "Sonoma"},
    {"name": "San Mateo", "lat": 37.5630, "lon": -122.3255, "region": "Bay Area", "county": "San Mateo"},
    {"name": "Daly City", "lat": 37.6879, "lon": -122.4702, "region": "Bay Area", "county": "San Mateo"},
    
    # CENTRAL VALLEY
    {"name": "Modesto", "lat": 37.6391, "lon": -120.9969, "region": "Central Valley", "county": "Stanislaus"},
    {"name": "Stockton", "lat": 37.9577, "lon": -121.2908, "region": "Central Valley", "county": "San Joaquin"},
    {"name": "Visalia", "lat": 36.3302, "lon": -119.2921, "region": "Central Valley", "county": "Tulare"},
    {"name": "Clovis", "lat": 36.8252, "lon": -119.7029, "region": "Central Valley", "county": "Fresno"},
    {"name": "Elk Grove", "lat": 38.4088, "lon": -121.3716, "region": "Central Valley", "county": "Sacramento"},
    {"name": "Roseville", "lat": 38.7521, "lon": -121.2880, "region": "Central Valley", "county": "Placer"},
    {"name": "Merced", "lat": 37.3022, "lon": -120.4830, "region": "Central Valley", "county": "Merced"},
    {"name": "Turlock", "lat": 37.4947, "lon": -120.8466, "region": "Central Valley", "county": "Stanislaus"},
    
    # CENTRAL COAST
    {"name": "Salinas", "lat": 36.6777, "lon": -121.6555, "region": "Central Coast", "county": "Monterey"},
    {"name": "Santa Barbara", "lat": 34.4208, "lon": -119.6982, "region": "Central Coast", "county": "Santa Barbara"},
    {"name": "Santa Cruz", "lat": 36.9741, "lon": -122.0308, "region": "Central Coast", "county": "Santa Cruz"},
    {"name": "San Luis Obispo", "lat": 35.2828, "lon": -120.6596, "region": "Central Coast", "county": "San Luis Obispo"},
    
    # NORTH REGIONS
    {"name": "Redding", "lat": 40.5865, "lon": -122.3917, "region": "North Valley", "county": "Shasta"},
    {"name": "Chico", "lat": 39.7285, "lon": -121.8375, "region": "North Valley", "county": "Butte"},
    {"name": "Eureka", "lat": 40.8021, "lon": -124.1637, "region": "North Coast", "county": "Humboldt"},
    
    # DESERT REGIONS
    {"name": "Palm Springs", "lat": 33.8303, "lon": -116.5453, "region": "Desert", "county": "Riverside"},
    {"name": "Indio", "lat": 33.7206, "lon": -116.2156, "region": "Desert", "county": "Riverside"},
    {"name": "Victorville", "lat": 34.5362, "lon": -117.2928, "region": "Desert", "county": "San Bernardino"},
]

@task
def extract_and_load_raw_historical_weather():
    
    cur = return_snowflake_conn()
    
    try:
        cur.execute(f"USE DATABASE {user_db}")
        cur.execute("USE SCHEMA RAW_ARCHIVE")
        cur.execute("DROP TABLE IF EXISTS WEATHER_CALIFORNIA_HISTORICAL_RAW")
        
        cur.execute("""
            CREATE TABLE WEATHER_CALIFORNIA_HISTORICAL_RAW (
                record_id VARCHAR(200) PRIMARY KEY,
                city VARCHAR(100) NOT NULL,
                state VARCHAR(50),
                region VARCHAR(100),
                county VARCHAR(100),
                latitude FLOAT,
                longitude FLOAT,
                timestamp_utc TIMESTAMP_NTZ,
                temperature_f FLOAT,
                relative_humidity_pct FLOAT,
                precipitation_mm FLOAT,
                rain_mm FLOAT,
                snowfall_mm FLOAT,
                cloud_cover_pct FLOAT,
                pressure_msl_hpa FLOAT,
                wind_speed_mph FLOAT,
                wind_direction_deg FLOAT,
                wind_gusts_mph FLOAT,
                weather_code INT,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                data_source VARCHAR(50) DEFAULT 'OpenMeteo_Archive_API'
            )
        """)
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=DAYS_BACK)
        
        total_records_inserted = 0
        total_api_calls = 0
        
        for city_idx, city in enumerate(CALIFORNIA_CITIES, 1):
            
            try:
                params = {
                    "latitude": city['lat'],
                    "longitude": city['lon'],
                    "start_date": start_date.strftime('%Y-%m-%d'),
                    "end_date": end_date.strftime('%Y-%m-%d'),
                    "hourly": "temperature_2m,relative_humidity_2m,precipitation,rain,snowfall,cloud_cover,pressure_msl,wind_speed_10m,wind_direction_10m,wind_gusts_10m,weather_code",
                    "temperature_unit": "fahrenheit",
                    "wind_speed_unit": "mph",
                    "precipitation_unit": "mm",
                    "timezone": "America/Los_Angeles"
                }
                
                # Use the variable URL
                response = requests.get(api_url, params=params, timeout=30)
                total_api_calls += 1
                
                if response.status_code != 200:
                    continue
                
                data = response.json()
                hourly = data.get('hourly', {})
                timestamps = hourly.get('time', [])
                                
                records = []
                for i, timestamp_str in enumerate(timestamps):
                    try:
                        timestamp_dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M')
                    except ValueError:
                        timestamp_dt = datetime.fromisoformat(timestamp_str.replace('Z', ''))
                    
                    def get_val(arr, idx, type_func=float):
                        try:
                            val = arr[idx] if idx < len(arr) else None
                            return type_func(val) if val is not None else None
                        except:
                            return None
                    
                    record_id = f"{city['name'].replace(' ', '_')}_{timestamp_str.replace(':', '').replace('-', '').replace('T', '_')}"
                    
                    records.append((
                        record_id,
                        city['name'],
                        TARGET_STATE,
                        city['region'],
                        city['county'],
                        city['lat'],
                        city['lon'],
                        timestamp_dt,
                        get_val(hourly.get('temperature_2m', []), i),
                        get_val(hourly.get('relative_humidity_2m', []), i),
                        get_val(hourly.get('precipitation', []), i),
                        get_val(hourly.get('rain', []), i),
                        get_val(hourly.get('snowfall', []), i),
                        get_val(hourly.get('cloud_cover', []), i),
                        get_val(hourly.get('pressure_msl', []), i),
                        get_val(hourly.get('wind_speed_10m', []), i),
                        get_val(hourly.get('wind_direction_10m', []), i),
                        get_val(hourly.get('wind_gusts_10m', []), i),
                        get_val(hourly.get('weather_code', []), i, int),
                    ))
                
                insert_sql = """
                    INSERT INTO WEATHER_CALIFORNIA_HISTORICAL_RAW (
                        record_id, city, state, region, county, latitude, longitude,
                        timestamp_utc, temperature_f, relative_humidity_pct,
                        precipitation_mm, rain_mm, snowfall_mm, cloud_cover_pct,
                        pressure_msl_hpa, wind_speed_mph, wind_direction_deg,
                        wind_gusts_mph, weather_code
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cur.executemany(insert_sql, records)
                total_records_inserted += len(records)
                
                time.sleep(0.2)
                
            except Exception as e:
                continue
        
        cur.execute("SELECT COUNT(*) FROM WEATHER_CALIFORNIA_HISTORICAL_RAW")
        final_count = cur.fetchone()[0]
        
    except Exception as e:
        raise
    finally:
        cur.close()

with DAG(
    dag_id="Weather_California_Historical_ELT_DBT_Meteo",
    start_date=datetime(2025, 1, 1),
    description="ELT: Load RAW California historical weather, Transform with dbt",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["ELT", "california", "weather", "historical", "dbt", "openmeteo"],
) as dag:

    load_task = extract_and_load_raw_historical_weather()
    
    dbt_deps = BashOperator(
        task_id="dbt_install_dependencies",
        bash_command=f"cd {DBT_PROJECT_DIR} && rm -rf dbt_packages* && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    dbt_integrated = BashOperator(
        task_id="dbt_run_integrated",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select integrated --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    load_task >> dbt_deps >> dbt_staging >> dbt_integrated >> dbt_marts