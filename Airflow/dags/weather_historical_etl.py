from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import logging
import time
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()


TARGET_STATE = "CA"
TARGET_STATE_FULL = "California"
DAYS_BACK = 5
USER_DB = Variable.get("USER_DB")

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
def extract():
   
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=DAYS_BACK)
    
    all_records = []
    total_api_calls = 0
    
    for city_idx, city in enumerate(CALIFORNIA_CITIES, 1):
        
        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
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
            
            response = requests.get(url, params=params, timeout=30)
            total_api_calls += 1
            
            if response.status_code != 200:
                continue
            
            data = response.json()
            hourly = data.get('hourly', {})
            timestamps = hourly.get('time', [])
                        
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
                
                all_records.append({
                    'record_id': record_id,
                    'city': city['name'],
                    'state': TARGET_STATE,
                    'region': city['region'],
                    'county': city['county'],
                    'latitude': city['lat'],
                    'longitude': city['lon'],
                    'timestamp_utc': timestamp_dt,
                    'temperature_f': get_val(hourly.get('temperature_2m', []), i),
                    'relative_humidity_pct': get_val(hourly.get('relative_humidity_2m', []), i),
                    'precipitation_mm': get_val(hourly.get('precipitation', []), i),
                    'rain_mm': get_val(hourly.get('rain', []), i),
                    'snowfall_mm': get_val(hourly.get('snowfall', []), i),
                    'cloud_cover_pct': get_val(hourly.get('cloud_cover', []), i),
                    'pressure_msl_hpa': get_val(hourly.get('pressure_msl', []), i),
                    'wind_speed_mph': get_val(hourly.get('wind_speed_10m', []), i),
                    'wind_direction_deg': get_val(hourly.get('wind_direction_10m', []), i),
                    'wind_gusts_mph': get_val(hourly.get('wind_gusts_10m', []), i),
                    'weather_code': get_val(hourly.get('weather_code', []), i, int),
                })
            
            time.sleep(0.2)  # Rate limiting
            
        except Exception as e:
            continue
    
    
    return {"records": all_records}


@task
def transform(data):
    
    records = data['records']
    df = pd.DataFrame(records)
    
    if df.empty:
        return {"records": []}
        
    # Convert timestamp to datetime if not already
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    
    # Add derived date/time columns
    df['date'] = df['timestamp_utc'].dt.date
    df['hour'] = df['timestamp_utc'].dt.hour
    df['day_of_week'] = df['timestamp_utc'].dt.dayofweek
    df['day_name'] = df['timestamp_utc'].dt.day_name()
    df['month'] = df['timestamp_utc'].dt.month
    df['year'] = df['timestamp_utc'].dt.year
    
    # Calculate temperature in Celsius for reference
    df['temperature_c'] = (df['temperature_f'] - 32) * 5/9
    
    # Add weather condition categories based on weather_code
    def categorize_weather(code):
        if pd.isna(code):
            return 'Unknown'
        code = int(code)
        if code == 0:
            return 'Clear'
        elif code in [1, 2, 3]:
            return 'Partly Cloudy'
        elif code in [45, 48]:
            return 'Fog'
        elif code in [51, 53, 55, 56, 57]:
            return 'Drizzle'
        elif code in [61, 63, 65, 66, 67]:
            return 'Rain'
        elif code in [71, 73, 75, 77]:
            return 'Snow'
        elif code in [80, 81, 82]:
            return 'Rain Showers'
        elif code in [85, 86]:
            return 'Snow Showers'
        elif code in [95, 96, 99]:
            return 'Thunderstorm'
        else:
            return 'Other'
    
    df['weather_condition'] = df['weather_code'].apply(categorize_weather)
    
    # Calculate heat index (simplified formula when temp > 80F and humidity > 40%)
    df['heat_index_f'] = df.apply(
        lambda row: (
            -42.379 + 
            2.04901523 * row['temperature_f'] + 
            10.14333127 * row['relative_humidity_pct'] - 
            0.22475541 * row['temperature_f'] * row['relative_humidity_pct'] - 
            0.00683783 * row['temperature_f']**2 - 
            0.05481717 * row['relative_humidity_pct']**2 + 
            0.00122874 * row['temperature_f']**2 * row['relative_humidity_pct'] + 
            0.00085282 * row['temperature_f'] * row['relative_humidity_pct']**2 - 
            0.00000199 * row['temperature_f']**2 * row['relative_humidity_pct']**2
        ) if (row['temperature_f'] > 80 and row['relative_humidity_pct'] > 40) 
        else row['temperature_f'],
        axis=1
    )
    
    # Flag extreme weather conditions
    df['is_extreme_heat'] = df['temperature_f'] > 100
    df['is_extreme_cold'] = df['temperature_f'] < 32
    df['is_high_wind'] = df['wind_speed_mph'] > 25
    df['is_precipitation'] = df['precipitation_mm'] > 0
    
    # Convert timestamp to string format for Snowflake
    df['timestamp_utc'] = df['timestamp_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['date'] = df['date'].astype(str)
    
    # Fill NaN values appropriately
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    return {"records": df.to_dict(orient='records')}


@task
def load(data, target_table: str):
    
    records = data['records']
    
    if not records:
        return
    
    conn = return_snowflake_conn()
    
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            
            # Create or replace table
            cur.execute(f"""
                CREATE OR REPLACE TABLE {target_table} (
                    record_id VARCHAR(200) PRIMARY KEY,
                    city VARCHAR(100) NOT NULL,
                    state VARCHAR(50),
                    region VARCHAR(100),
                    county VARCHAR(100),
                    latitude FLOAT,
                    longitude FLOAT,
                    timestamp_utc TIMESTAMP_NTZ,
                    date DATE,
                    hour INT,
                    day_of_week INT,
                    day_name VARCHAR(20),
                    month INT,
                    year INT,
                    temperature_f FLOAT,
                    temperature_c FLOAT,
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
                    weather_condition VARCHAR(50),
                    heat_index_f FLOAT,
                    is_extreme_heat BOOLEAN,
                    is_extreme_cold BOOLEAN,
                    is_high_wind BOOLEAN,
                    is_precipitation BOOLEAN,
                    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    data_source VARCHAR(50) DEFAULT 'OpenMeteo_Archive_API'
                )
            """)
            
            # Clear existing data
            cur.execute(f"DELETE FROM {target_table}")
            
            # Insert data
            insert_sql = f"""
                INSERT INTO {target_table} (
                    record_id, city, state, region, county, latitude, longitude,
                    timestamp_utc, date, hour, day_of_week, day_name, month, year,
                    temperature_f, temperature_c, relative_humidity_pct,
                    precipitation_mm, rain_mm, snowfall_mm, cloud_cover_pct,
                    pressure_msl_hpa, wind_speed_mph, wind_direction_deg,
                    wind_gusts_mph, weather_code, weather_condition, heat_index_f,
                    is_extreme_heat, is_extreme_cold, is_high_wind, is_precipitation
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """
            
            params = [
                (
                    r['record_id'], r['city'], r['state'], r['region'], r['county'],
                    r['latitude'], r['longitude'], r['timestamp_utc'], r['date'],
                    r['hour'], r['day_of_week'], r['day_name'], r['month'], r['year'],
                    r['temperature_f'], r['temperature_c'], r['relative_humidity_pct'],
                    r['precipitation_mm'], r['rain_mm'], r['snowfall_mm'],
                    r['cloud_cover_pct'], r['pressure_msl_hpa'], r['wind_speed_mph'],
                    r['wind_direction_deg'], r['wind_gusts_mph'], r['weather_code'],
                    r['weather_condition'], r['heat_index_f'], r['is_extreme_heat'],
                    r['is_extreme_cold'], r['is_high_wind'], r['is_precipitation']
                )
                for r in records
            ]
            
            if params:
                cur.executemany(insert_sql, params)
            
            cur.execute("COMMIT")
            
            # Verify load
            cur.execute(f"SELECT COUNT(*) FROM {target_table}")
            final_count = cur.fetchone()[0]
            
            
    except Exception as e:
        try:
            with conn.cursor() as cur:
                cur.execute("ROLLBACK")
        finally:
            conn.close()
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


# DAG definition
with DAG(
    dag_id="Weather_California_Historical_ETL_OpenMeteo",
    start_date=datetime(2025, 1, 1),
    description="ETL: Extract from Open-Meteo, Transform in Python, Load to Snowflake",
    catchup=False,
    schedule="0 2 * * *",  # Run daily at 2 AM
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    tags=["ETL", "california", "weather", "historical", "openmeteo"],
) as dag:

       
    # Target table in Snowflake
    target_table = f"{USER_DB}.RAW.WEATHER_CALIFORNIA_HISTORICAL_ETL"
    
    # Define ETL task flow
    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task, target_table)
    
    # Set dependencies
    extract_task >> transform_task >> load_task