
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import logging
import json
import time
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
USER_DB = Variable.get("USER_DB")


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn()


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
    
    weather_records = []
    api_calls = 0
    failed_calls = 0
    
    for idx, city in enumerate(CALIFORNIA_CITIES, 1):
        try:
            logger.info(f"[{idx}/{len(CALIFORNIA_CITIES)}] Fetching: {city['name']}, {city['county']} County...")
            
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": city['lat'],
                "longitude": city['lon'],
                "current": "temperature_2m,relative_humidity_2m,apparent_temperature,precipitation,rain,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m",
                "temperature_unit": "fahrenheit",
                "wind_speed_unit": "mph",
                "precipitation_unit": "inch",
                "timezone": "America/Los_Angeles"
            }
            
            response = requests.get(url, params=params, timeout=15)
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
                    'timestamp_utc': dt,
                    'date': dt.date(),
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
            
            time.sleep(0.2)  # Rate limiting
                
        except Exception as e:
            failed_calls += 1
            logger.error(f"  ❌ Error for {city['name']}: {str(e)[:100]}")
            continue
    
   
    
    return {
        "records": weather_records,
        "api_calls": api_calls,
        "failed_calls": failed_calls
    }


@task
def transform(data):
    
    records = data['records']
    df = pd.DataFrame(records)
    
    if df.empty:
        logger.warning("No records to transform")
        return {"records": []}
    
    
    # Convert timestamp to datetime if not already
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    df['date'] = pd.to_datetime(df['date'])
    
    # Add time-based features
    df['day_of_week'] = df['timestamp_utc'].dt.dayofweek
    df['day_name'] = df['timestamp_utc'].dt.day_name()
    df['month'] = df['timestamp_utc'].dt.month
    df['month_name'] = df['timestamp_utc'].dt.month_name()
    df['year'] = df['timestamp_utc'].dt.year
    df['is_weekend'] = df['day_of_week'].isin([5, 6])
    
    # Time of day classification
    def classify_time_of_day(hour):
        if 6 <= hour < 12:
            return 'Morning'
        elif 12 <= hour < 18:
            return 'Afternoon'
        elif 18 <= hour < 22:
            return 'Evening'
        else:
            return 'Night'
    
    df['time_of_day'] = df['hour'].apply(classify_time_of_day)
    
    # Temperature conversions and metrics
    df['temperature_c'] = (df['temperature_f'] - 32) * 5/9
    df['feels_like_c'] = (df['feels_like_f'] - 32) * 5/9
    df['temp_feels_diff'] = df['temperature_f'] - df['feels_like_f']
    
    # Calculate heat index (when temp > 80F and humidity > 40%)
    df['heat_index_f'] = df.apply(
        lambda row: (
            -42.379 + 
            2.04901523 * row['temperature_f'] + 
            10.14333127 * row['humidity_pct'] - 
            0.22475541 * row['temperature_f'] * row['humidity_pct'] - 
            0.00683783 * row['temperature_f']**2 - 
            0.05481717 * row['humidity_pct']**2 + 
            0.00122874 * row['temperature_f']**2 * row['humidity_pct'] + 
            0.00085282 * row['temperature_f'] * row['humidity_pct']**2 - 
            0.00000199 * row['temperature_f']**2 * row['humidity_pct']**2
        ) if (pd.notna(row['temperature_f']) and pd.notna(row['humidity_pct']) and 
              row['temperature_f'] > 80 and row['humidity_pct'] > 40) 
        else row['temperature_f'],
        axis=1
    )
    
    # Wind chill (when temp < 50F and wind > 3mph)
    df['wind_chill_f'] = df.apply(
        lambda row: (
            35.74 + 
            0.6215 * row['temperature_f'] - 
            35.75 * (row['wind_speed_mph'] ** 0.16) + 
            0.4275 * row['temperature_f'] * (row['wind_speed_mph'] ** 0.16)
        ) if (pd.notna(row['temperature_f']) and pd.notna(row['wind_speed_mph']) and 
              row['temperature_f'] < 50 and row['wind_speed_mph'] > 3) 
        else row['temperature_f'],
        axis=1
    )
    
    # Convert precipitation from inches to mm for consistency
    df['precipitation_mm'] = df['precipitation_in'] * 25.4
    df['rain_mm'] = df['rain_in'] * 25.4
    df['snowfall_mm'] = df['snowfall_in'] * 25.4
    
    # Weather condition classifications
    df['is_clear'] = df['weather_main'] == 'Clear'
    df['is_cloudy'] = df['weather_main'] == 'Clouds'
    df['is_rainy'] = df['weather_main'].isin(['Rain', 'Drizzle'])
    df['is_stormy'] = df['weather_main'] == 'Thunderstorm'
    df['is_snowy'] = df['weather_main'] == 'Snow'
    df['is_foggy'] = df['weather_main'] == 'Fog'
    
    # Comfort level classification
    def classify_comfort(row):
        temp = row['temperature_f']
        humidity = row['humidity_pct']
        
        if pd.isna(temp) or pd.isna(humidity):
            return 'Unknown'
        
        if temp < 32:
            return 'Very Cold'
        elif temp < 50:
            return 'Cold'
        elif 50 <= temp <= 75:
            if humidity < 60:
                return 'Comfortable'
            else:
                return 'Humid'
        elif 75 < temp <= 85:
            if humidity < 60:
                return 'Warm'
            else:
                return 'Hot & Humid'
        elif 85 < temp <= 95:
            return 'Very Hot'
        else:
            return 'Extreme Heat'
    
    df['comfort_level'] = df.apply(classify_comfort, axis=1)
    
    # Flag extreme conditions
    df['is_extreme_heat'] = df['temperature_f'] > 100
    df['is_extreme_cold'] = df['temperature_f'] < 32
    df['is_high_wind'] = df['wind_speed_mph'] > 25
    df['is_high_humidity'] = df['humidity_pct'] > 80
    df['is_low_visibility'] = df['weather_main'].isin(['Fog', 'Snow'])
    df['is_precipitation'] = df['precipitation_in'] > 0
    
    # Wind direction classification
    def classify_wind_direction(deg):
        if pd.isna(deg):
            return 'Unknown'
        if deg < 22.5 or deg >= 337.5:
            return 'N'
        elif deg < 67.5:
            return 'NE'
        elif deg < 112.5:
            return 'E'
        elif deg < 157.5:
            return 'SE'
        elif deg < 202.5:
            return 'S'
        elif deg < 247.5:
            return 'SW'
        elif deg < 292.5:
            return 'W'
        else:
            return 'NW'
    
    df['wind_direction_cardinal'] = df['wind_direction_deg'].apply(classify_wind_direction)
    
    # Air quality index estimate (simplified based on pressure and humidity)
    df['aqi_estimate'] = df.apply(
        lambda row: (
            'Good' if row['pressure_msl_hpa'] > 1013 and row['humidity_pct'] < 70 
            else 'Moderate' if row['pressure_msl_hpa'] > 1000 
            else 'Poor'
        ),
        axis=1
    )
    
    # Convert timestamps to strings for Snowflake
    df['timestamp_utc'] = df['timestamp_utc'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')
    
    # Fill NaN values appropriately
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    string_cols = df.select_dtypes(include=['object']).columns
    df[string_cols] = df[string_cols].fillna('Unknown')
    
   
    
    return {
        "records": df.to_dict(orient='records'),
        "stats": {
            "total_records": len(df),
            "cities": df['city'].nunique(),
            "avg_temp": df['temperature_f'].mean(),
            "max_temp": df['temperature_f'].max(),
            "min_temp": df['temperature_f'].min()
        }
    }


@task
def load(data, target_table: str):
   
    
    records = data['records']
    
    if not records:
        logger.warning("No records to load")
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
                    month_name VARCHAR(20),
                    year INT,
                    is_weekend BOOLEAN,
                    time_of_day VARCHAR(20),
                    temperature_f FLOAT,
                    temperature_c FLOAT,
                    feels_like_f FLOAT,
                    feels_like_c FLOAT,
                    temp_feels_diff FLOAT,
                    temp_min_f FLOAT,
                    temp_max_f FLOAT,
                    pressure_msl_hpa FLOAT,
                    surface_pressure_hpa FLOAT,
                    humidity_pct FLOAT,
                    wind_speed_mph FLOAT,
                    wind_direction_deg FLOAT,
                    wind_direction_cardinal VARCHAR(5),
                    wind_gusts_mph FLOAT,
                    cloud_cover_pct FLOAT,
                    precipitation_in FLOAT,
                    precipitation_mm FLOAT,
                    rain_in FLOAT,
                    rain_mm FLOAT,
                    snowfall_in FLOAT,
                    snowfall_mm FLOAT,
                    weather_code INT,
                    weather_main VARCHAR(50),
                    weather_description VARCHAR(200),
                    weather_icon VARCHAR(20),
                    heat_index_f FLOAT,
                    wind_chill_f FLOAT,
                    is_clear BOOLEAN,
                    is_cloudy BOOLEAN,
                    is_rainy BOOLEAN,
                    is_stormy BOOLEAN,
                    is_snowy BOOLEAN,
                    is_foggy BOOLEAN,
                    comfort_level VARCHAR(50),
                    is_extreme_heat BOOLEAN,
                    is_extreme_cold BOOLEAN,
                    is_high_wind BOOLEAN,
                    is_high_humidity BOOLEAN,
                    is_low_visibility BOOLEAN,
                    is_precipitation BOOLEAN,
                    aqi_estimate VARCHAR(20),
                    raw_json VARCHAR,
                    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    data_source VARCHAR(50) DEFAULT 'OpenMeteo_Current_API_ETL'
                )
            """)
           
            # Clear existing data
            cur.execute(f"DELETE FROM {target_table}")
            
            insert_sql = f"""
                INSERT INTO {target_table} (
                    record_id, city, state, region, county, latitude, longitude,
                    timestamp_utc, date, hour, day_of_week, day_name, month, month_name,
                    year, is_weekend, time_of_day, temperature_f, temperature_c,
                    feels_like_f, feels_like_c, temp_feels_diff, temp_min_f, temp_max_f,
                    pressure_msl_hpa, surface_pressure_hpa, humidity_pct, wind_speed_mph,
                    wind_direction_deg, wind_direction_cardinal, wind_gusts_mph,
                    cloud_cover_pct, precipitation_in, precipitation_mm, rain_in, rain_mm,
                    snowfall_in, snowfall_mm, weather_code, weather_main,
                    weather_description, weather_icon, heat_index_f, wind_chill_f,
                    is_clear, is_cloudy, is_rainy, is_stormy, is_snowy, is_foggy,
                    comfort_level, is_extreme_heat, is_extreme_cold, is_high_wind,
                    is_high_humidity, is_low_visibility, is_precipitation,
                    aqi_estimate, raw_json
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """
            
            params = []
            for r in records:
                params.append((
                    r['record_id'],           
                    r['city'],                
                    r['state'],               
                    r['region'],              
                    r['county'],              
                    r['latitude'],            
                    r['longitude'],           
                    r['timestamp_utc'],       
                    r['date'],                
                    r['hour'],                
                    r['day_of_week'],         
                    r['day_name'],            
                    r['month'],               
                    r['month_name'],          
                    r['year'],                
                    r['is_weekend'],          
                    r['time_of_day'],         
                    r['temperature_f'],       
                    r['temperature_c'],       
                    r['feels_like_f'],        
                    r['feels_like_c'],        
                    r['temp_feels_diff'],     
                    r['temp_min_f'],          
                    r['temp_max_f'],          
                    r['pressure_msl_hpa'],    
                    r['surface_pressure_hpa'],
                    r['humidity_pct'],        
                    r['wind_speed_mph'],      
                    r['wind_direction_deg'],  
                    r['wind_direction_cardinal'], 
                    r['wind_gusts_mph'],      
                    r['cloud_cover_pct'],     
                    r['precipitation_in'],    
                    r['precipitation_mm'],    
                    r['rain_in'],             
                    r['rain_mm'],             
                    r['snowfall_in'],         
                    r['snowfall_mm'],         
                    r['weather_code'],        
                    r['weather_main'],        
                    r['weather_description'], 
                    r['weather_icon'],        
                    r['heat_index_f'],        
                    r['wind_chill_f'],        
                    r['is_clear'],            
                    r['is_cloudy'],           
                    r['is_rainy'],            
                    r['is_stormy'],           
                    r['is_snowy'],            
                    r['is_foggy'],            
                    r['comfort_level'],       
                    r['is_extreme_heat'],     
                    r['is_extreme_cold'],     
                    r['is_high_wind'],        
                    r['is_high_humidity'],    
                    r['is_low_visibility'],   
                    r['is_precipitation'],    
                    r['aqi_estimate'],        
                    r['raw_json']             
                ))
            
            if params:
                cur.executemany(insert_sql, params)
            
            cur.execute("COMMIT")
            
            # Verify load
            cur.execute(f"SELECT COUNT(*) FROM {target_table}")
            final_count = cur.fetchone()[0]
            
            # Get sample statistics
            cur.execute(f"""
                SELECT 
                    AVG(temperature_f) as avg_temp,
                    MAX(temperature_f) as max_temp,
                    MIN(temperature_f) as min_temp,
                    COUNT(DISTINCT city) as city_count
                FROM {target_table}
            """)
            stats = cur.fetchone()
            
            
    except Exception as e:
        logger.error(f"\n LOAD ERROR: {str(e)}")
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
    dag_id="Realtime_Weather_California_ETL_OpenMeteo",
    start_date=datetime(2025, 1, 1),
    description="Real-time Weather ETL: Extract from Open-Meteo, Transform in Python, Load to Snowflake",
    catchup=False,
    schedule="*/30 * * * *",  # Run every 30 minutes
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["ETL", "realtime", "weather", "california", "openmeteo", "free"],
) as dag:

    # Target table in Snowflake
    target_table = f"{USER_DB}.RAW.WEATHER_CALIFORNIA_REALTIME_ETL"
    # Define ETL task flow
    extract_task = extract()
    transform_task = transform(extract_task)
    load_task = load(transform_task, target_table)
    
    # Set dependencies
    extract_task >> transform_task >> load_task