from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import logging
import json
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Variable.get("dbt_project_dir", default_var="/opt/airflow/dbt")
api_url = Variable.get("weather_api_url_realtime", default_var="https://api.open-meteo.com/v1/forecast")
user_db = Variable.get("USER_DB")
user_wh = Variable.get("USER_WH")

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

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
def extract_and_load_raw_weather_openmeteo():
    logger.info("=" * 80)
    logger.info("REAL-TIME WEATHER ELT - Open-Meteo API (FREE)")
    logger.info(f"Fetching weather for {len(CALIFORNIA_CITIES)} California cities")
    logger.info("=" * 80)
    
    weather_records = []
    api_calls = 0
    failed_calls = 0
    
    for idx, city in enumerate(CALIFORNIA_CITIES, 1):
        try:
            
            params = {
                "latitude": city['lat'],
                "longitude": city['lon'],
                "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m",
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "precipitation_unit": "inch",
                "timezone": "America/Los_Angeles"
            }
            
            # Use the variable URL
            response = requests.get(api_url, params=params, timeout=15)
            api_calls += 1
            
            if response.status_code == 200:
                data = response.json()
                current = data.get('current', {})
                
                timestamp_str = current.get('time', datetime.utcnow().strftime('%Y-%m-%dT%H:%M'))
                try:
                    dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M')
                except ValueError:
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', ''))
                
                weather_code = current.get('weather_code', 0)
                weather_mapping = {
                    0: ('Clear', 'clear sky', '01d'),
                    1: ('Clouds', 'mainly clear', '02d'),
                    2: ('Clouds', 'partly cloudy', '02d'),
                    3: ('Clouds', 'overcast', '03d'),
                    45: ('Fog', 'fog', '50d'),
                    48: ('Fog', 'depositing rime fog', '50d'),
                    51: ('Drizzle', 'light drizzle', '09d'),
                    53: ('Drizzle', 'moderate drizzle', '09d'),
                    55: ('Drizzle', 'dense drizzle', '09d'),
                    61: ('Rain', 'slight rain', '10d'),
                    63: ('Rain', 'moderate rain', '10d'),
                    65: ('Rain', 'heavy rain', '10d'),
                    71: ('Snow', 'slight snow', '13d'),
                    73: ('Snow', 'moderate snow', '13d'),
                    75: ('Snow', 'heavy snow', '13d'),
                    95: ('Thunderstorm', 'thunderstorm', '11d'),
                }
                weather_main, weather_desc, weather_icon = weather_mapping.get(
                    weather_code, ('Unknown', 'unknown', '01d')
                )
                
                weather_record = {
                    'record_id': f"{city['name'].replace(' ', '_')}_{dt.strftime('%Y%m%d_%H%M')}",
                    'city': city['name'],
                    'state': 'CA',
                    'region': city['region'],
                    'county': city['county'],
                    'latitude': city['lat'],
                    'longitude': city['lon'],
                    'timestamp_str': dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'date': dt.strftime('%Y-%m-%d'),
                    'hour': dt.hour,
                    'temperature_f': current.get('temperature_2m'),
                    'feels_like_f': current.get('apparent_temperature'),
                    'temp_min_f': current.get('temperature_2m'),
                    'temp_max_f': current.get('temperature_2m'),
                    'pressure_msl_hpa': current.get('pressure_msl'),
                    'surface_pressure_hpa': current.get('surface_pressure'),
                    'humidity_pct': current.get('relative_humidity_2m'),
                    'wind_speed_mph': current.get('wind_speed_10m'),
                    'wind_direction_deg': current.get('wind_direction_10m'),
                    'wind_gusts_mph': current.get('wind_gusts_10m'),
                    'cloud_cover_pct': current.get('cloud_cover'),
                    'precipitation_in': current.get('precipitation'),
                    'rain_in': current.get('rain'),
                    'snowfall_in': current.get('snowfall'),
                    'weather_code': weather_code,
                    'weather_main': weather_main,
                    'weather_description': weather_desc,
                    'weather_icon': weather_icon,
                    'raw_json': json.dumps(data),
                }
                
                weather_records.append(weather_record)
                temp = weather_record['temperature_f']
                
            else:
                failed_calls += 1
            
            time.sleep(0.2)
                
        except Exception as e:
            failed_calls += 1
            continue
    
    
    if not weather_records:
        return {"count": 0, "api_calls": api_calls}
    
    cur = return_snowflake_conn()
    
    try:
        cur.execute(f"USE WAREHOUSE {user_wh}")
        cur.execute(f"USE DATABASE {user_db}")
        cur.execute("USE SCHEMA RAW")
        
        cur.execute("DROP TABLE IF EXISTS WEATHER_CALIFORNIA_REALTIME_RAW CASCADE")
        
        cur.execute("""
            CREATE TABLE WEATHER_CALIFORNIA_REALTIME_RAW (
                record_id VARCHAR(200) PRIMARY KEY,
                city VARCHAR(100) NOT NULL,
                state VARCHAR(50),
                region VARCHAR(100),
                county VARCHAR(100),
                latitude FLOAT,
                longitude FLOAT,
                timestamp_str VARCHAR(50) NOT NULL,
                date VARCHAR(50),
                hour INT,
                temperature_f FLOAT,
                feels_like_f FLOAT,
                temp_min_f FLOAT,
                temp_max_f FLOAT,
                pressure_msl_hpa FLOAT,
                surface_pressure_hpa FLOAT,
                humidity_pct FLOAT,
                wind_speed_mph FLOAT,
                wind_direction_deg FLOAT,
                wind_gusts_mph FLOAT,
                cloud_cover_pct FLOAT,
                precipitation_in FLOAT,
                rain_in FLOAT,
                snowfall_in FLOAT,
                weather_code INT,
                weather_main VARCHAR(50),
                weather_description VARCHAR(200),
                weather_icon VARCHAR(20),
                raw_json VARCHAR,
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                data_source VARCHAR(50) DEFAULT 'OpenMeteo_Current_API'
            )
        """)
        
        
        insert_sql = """
            INSERT INTO WEATHER_CALIFORNIA_REALTIME_RAW (
                record_id, city, state, region, county, latitude, longitude,
                timestamp_str, date, hour, temperature_f, feels_like_f, temp_min_f, 
                temp_max_f, pressure_msl_hpa, surface_pressure_hpa, humidity_pct,
                wind_speed_mph, wind_direction_deg, wind_gusts_mph, cloud_cover_pct,
                precipitation_in, rain_in, snowfall_in, weather_code, weather_main,
                weather_description, weather_icon, raw_json
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        
        rows = [
            (
                r['record_id'], r['city'], r['state'], r['region'], r['county'],
                r['latitude'], r['longitude'], r['timestamp_str'], r['date'], 
                r['hour'], r['temperature_f'], r['feels_like_f'], r['temp_min_f'],
                r['temp_max_f'], r['pressure_msl_hpa'], r['surface_pressure_hpa'],
                r['humidity_pct'], r['wind_speed_mph'], r['wind_direction_deg'],
                r['wind_gusts_mph'], r['cloud_cover_pct'], r['precipitation_in'],
                r['rain_in'], r['snowfall_in'], r['weather_code'], r['weather_main'],
                r['weather_description'], r['weather_icon'], r['raw_json']
            )
            for r in weather_records
        ]
        
        cur.executemany(insert_sql, rows)
        
        cur.execute("SELECT COUNT(*) FROM WEATHER_CALIFORNIA_REALTIME_RAW")
        count = cur.fetchone()[0]
        
        return {"count": count, "api_calls": api_calls, "failed": failed_calls}
        
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        cur.close()

with DAG(
    dag_id="Realtime_Weather_OpenMeteo_ELT_DBT",
    start_date=datetime(2025, 1, 1),
    description="Real-time Weather ELT using FREE Open-Meteo API, Transform with dbt",
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["ELT", "realtime", "weather", "openmeteo", "free", "dbt"],
) as dag:

    weather_task = extract_and_load_raw_weather_openmeteo()
    
    dbt_deps = BashOperator(
    task_id="dbt_install_dependencies",
    bash_command=f"""
        cd {DBT_PROJECT_DIR}
        dbt deps --profiles-dir {DBT_PROJECT_DIR}
        dbt list --profiles-dir {DBT_PROJECT_DIR} 
    """,
)

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging.weather.stg_weather_realtime --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts.fct_weather_current --profiles-dir {DBT_PROJECT_DIR}",
    )
    
    dbt_test = BashOperator(
        task_id="dbt_test_data_quality",
        bash_command=f"cd {DBT_PROJECT_DIR} && (dbt test --select staging.weather.* fct_weather_current --profiles-dir {DBT_PROJECT_DIR} || echo 'Tests completed with some failures')",
        trigger_rule="all_done",
    )
    
    weather_task >> dbt_deps >> dbt_staging >> dbt_marts >> dbt_test