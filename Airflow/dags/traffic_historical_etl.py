
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

# Path to downloaded Kaggle dataset
KAGGLE_DATA_PATH = "/opt/airflow/data/US_Accidents_March23.csv"

# Filter for California only
TARGET_STATE = 'CA'
USER_DB = Variable.get("USER_DB")
USER_WH = Variable.get("USER_WH")

@task
def extract_kaggle_accidents():
   
    
    if not os.path.exists(KAGGLE_DATA_PATH):
        raise FileNotFoundError(f"Missing file: {KAGGLE_DATA_PATH}")
    
    
    # Read CSV in chunks - reduced for better performance
    chunk_size = 10000
    chunks = []
    
    for i, chunk in enumerate(pd.read_csv(KAGGLE_DATA_PATH, chunksize=chunk_size)):
        
        # Filter for California state only
        chunk = chunk[chunk['State'] == TARGET_STATE]
        
        if not chunk.empty:
            chunks.append(chunk)
     
    if not chunks:
        return {"records": []}
    
    df = pd.concat(chunks, ignore_index=True)
  
    return {"records": df.to_dict(orient="records"), "count": len(df)}


@task
def transform_accidents_data(extract_output):
   
    
    records = extract_output.get("records", [])
    
    if not records:
        logger.warning("No records to transform")
        return {"records": []}
    
    df = pd.DataFrame(records)
    logger.info(f"Transforming {len(df)} records...")
    
    # Standardize column names and select relevant columns
    transformed = pd.DataFrame({
        'accident_id': df['ID'],
        'severity': df['Severity'],
        'start_time': pd.to_datetime(df['Start_Time'], errors='coerce', format='mixed'),
        'end_time': pd.to_datetime(df['End_Time'], errors='coerce', format='mixed'),
        'start_lat': df['Start_Lat'],
        'start_lng': df['Start_Lng'],
        'distance_mi': df['Distance(mi)'],
        'description': df['Description'],
        'street': df['Street'],
        'city': df['City'],
        'county': df['County'],
        'state': df['State'],
        'zipcode': df['Zipcode'],
        'country': df['Country'],
        'timezone': df['Timezone'],
        
        # Weather conditions
        'weather_timestamp': pd.to_datetime(df['Weather_Timestamp'], errors='coerce', format='mixed'),
        'temperature_f': df['Temperature(F)'],
        'wind_chill_f': df['Wind_Chill(F)'],
        'humidity_pct': df['Humidity(%)'],
        'pressure_in': df['Pressure(in)'],
        'visibility_mi': df['Visibility(mi)'],
        'wind_direction': df['Wind_Direction'],
        'wind_speed_mph': df['Wind_Speed(mph)'],
        'precipitation_in': df['Precipitation(in)'],
        'weather_condition': df['Weather_Condition'],
        
        # Road features (convert to boolean)
        'amenity': df['Amenity'].fillna(False).astype(bool),
        'bump': df['Bump'].fillna(False).astype(bool),
        'crossing': df['Crossing'].fillna(False).astype(bool),
        'give_way': df['Give_Way'].fillna(False).astype(bool),
        'junction': df['Junction'].fillna(False).astype(bool),
        'no_exit': df['No_Exit'].fillna(False).astype(bool),
        'railway': df['Railway'].fillna(False).astype(bool),
        'roundabout': df['Roundabout'].fillna(False).astype(bool),
        'station': df['Station'].fillna(False).astype(bool),
        'stop': df['Stop'].fillna(False).astype(bool),
        'traffic_calming': df['Traffic_Calming'].fillna(False).astype(bool),
        'traffic_signal': df['Traffic_Signal'].fillna(False).astype(bool),
        'turning_loop': df['Turning_Loop'].fillna(False).astype(bool),
        
        # Timing
        'sunrise_sunset': df['Sunrise_Sunset'],
        'civil_twilight': df['Civil_Twilight'],
        'nautical_twilight': df['Nautical_Twilight'],
        'astronomical_twilight': df['Astronomical_Twilight'],
    })
    
    # Data validation
    transformed = transformed[
        (transformed['start_lat'].between(-90, 90)) &
        (transformed['start_lng'].between(-180, 180)) &
        (transformed['severity'].between(1, 4))
    ]
    
    # Handle missing values and convert NaN to None for proper SQL NULL handling
    transformed['distance_mi'] = transformed['distance_mi'].fillna(0)
    transformed['temperature_f'] = transformed['temperature_f'].fillna(70)
    transformed['visibility_mi'] = transformed['visibility_mi'].fillna(10)
    transformed['wind_speed_mph'] = transformed['wind_speed_mph'].fillna(0)
    transformed['precipitation_in'] = transformed['precipitation_in'].fillna(0)
    transformed['humidity_pct'] = transformed['humidity_pct'].fillna(50)
    transformed['pressure_in'] = transformed['pressure_in'].fillna(29.92)
    
    # Fill NaN in string columns with empty string or default values
    string_columns = ['description', 'street', 'city', 'county', 'state', 'zipcode', 
                     'country', 'timezone', 'wind_direction', 'weather_condition',
                     'sunrise_sunset', 'civil_twilight', 'nautical_twilight', 'astronomical_twilight']
    for col in string_columns:
        if col in transformed.columns:
            transformed[col] = transformed[col].fillna('')
    
    # Replace any remaining NaN values with None for SQL NULL compatibility
    transformed = transformed.replace({pd.NA: None, pd.NaT: None})
    transformed = transformed.where(pd.notna(transformed), None)
    
    # Convert timestamps to strings for JSON serialization, handling None values
    for col in ['start_time', 'end_time', 'weather_timestamp']:
        transformed[col] = transformed[col].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) and x is not None else None
        )
    
    # Convert to dictionary and clean any remaining NaN values
    records_dict = transformed.to_dict(orient="records")
    
    # Final NaN cleanup - replace any NaN with None in the dictionary
    import math
    cleaned_records = []
    for record in records_dict:
        cleaned_record = {}
        for key, value in record.items():
            # Check for NaN using math.isnan for floats, or pd.isna for any type
            if value is None:
                cleaned_record[key] = None
            elif isinstance(value, float) and math.isnan(value):
                cleaned_record[key] = None
            elif pd.isna(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)
    
    return {"records": cleaned_records, "count": len(cleaned_records)}


@task
def load_to_snowflake(data):

    records = data.get("records", [])
    
    if not records:
        return
    
    
    cur = return_snowflake_conn()
    
    try:
        cur.execute("BEGIN;")
        
        # Use the specified database and warehouse
        cur.execute(f"USE DATABASE {USER_DB}")
        cur.execute(f"USE WAREHOUSE {USER_WH}")
        
        # Create schema if it doesn't exist
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW_ARCHIVE")
        cur.execute("USE SCHEMA RAW_ARCHIVE")
        
        # Ensure table exists FIRST
        cur.execute("""
            CREATE TABLE IF NOT EXISTS TRAFFIC_ACCIDENTS_HISTORICAL (
                accident_id VARCHAR(50) NOT NULL,
                severity NUMBER(1),
                start_time TIMESTAMP_NTZ,
                end_time TIMESTAMP_NTZ,
                start_lat FLOAT,
                start_lng FLOAT,
                distance_mi FLOAT,
                description VARCHAR(5000),
                street VARCHAR(500),
                city VARCHAR(100),
                county VARCHAR(100),
                state VARCHAR(50),
                zipcode VARCHAR(20),
                country VARCHAR(50),
                timezone VARCHAR(50),
                weather_timestamp TIMESTAMP_NTZ,
                temperature_f FLOAT,
                wind_chill_f FLOAT,
                humidity_pct FLOAT,
                pressure_in FLOAT,
                visibility_mi FLOAT,
                wind_direction VARCHAR(20),
                wind_speed_mph FLOAT,
                precipitation_in FLOAT,
                weather_condition VARCHAR(100),
                amenity BOOLEAN,
                bump BOOLEAN,
                crossing BOOLEAN,
                give_way BOOLEAN,
                junction BOOLEAN,
                no_exit BOOLEAN,
                railway BOOLEAN,
                roundabout BOOLEAN,
                station BOOLEAN,
                stop BOOLEAN,
                traffic_calming BOOLEAN,
                traffic_signal BOOLEAN,
                turning_loop BOOLEAN,
                sunrise_sunset VARCHAR(20),
                civil_twilight VARCHAR(20),
                nautical_twilight VARCHAR(20),
                astronomical_twilight VARCHAR(20),
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                data_source VARCHAR(50) DEFAULT 'Kaggle',
                PRIMARY KEY (accident_id)
            )
        """)
        
        # IDEMPOTENCY CHECK - Check if data already loaded today (AFTER table exists)
        load_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            cur.execute("""
                SELECT COUNT(*) FROM TRAFFIC_ACCIDENTS_HISTORICAL 
                WHERE DATE(load_timestamp) = %s
                LIMIT 1
            """, (load_date,))
            
            existing_load = cur.fetchone()
            
            if existing_load and existing_load[0] > 0:
                logger.warning(f"⚠️  Data already loaded on {load_date}. Skipping to maintain idempotency.")
                logger.info("To reload, manually delete today's records or run tomorrow.")
                cur.execute("COMMIT;")
                return
        except Exception as check_error:
            logger.info(f"Idempotency check skipped (table may be empty): {check_error}")
        
        # FULL REFRESH - Delete existing data
        cur.execute("DELETE FROM TRAFFIC_ACCIDENTS_HISTORICAL")
        
        # Batch insert
        insert_sql = """
            INSERT INTO TRAFFIC_ACCIDENTS_HISTORICAL (
                accident_id, severity, start_time, end_time,
                start_lat, start_lng, distance_mi, description,
                street, city, county, state, zipcode, country, timezone,
                weather_timestamp, temperature_f, wind_chill_f,
                humidity_pct, pressure_in, visibility_mi,
                wind_direction, wind_speed_mph, precipitation_in, weather_condition,
                amenity, bump, crossing, give_way, junction, no_exit,
                railway, roundabout, station, stop, traffic_calming,
                traffic_signal, turning_loop,
                sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """
        
        batch_size = 1000
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            rows = [
                (
                    r['accident_id'], r['severity'], r['start_time'], r['end_time'],
                    r['start_lat'], r['start_lng'], r['distance_mi'], r['description'],
                    r['street'], r['city'], r['county'], r['state'], r['zipcode'], r['country'], r['timezone'],
                    r['weather_timestamp'], r['temperature_f'], r['wind_chill_f'],
                    r['humidity_pct'], r['pressure_in'], r['visibility_mi'],
                    r['wind_direction'], r['wind_speed_mph'], r['precipitation_in'], r['weather_condition'],
                    r['amenity'], r['bump'], r['crossing'], r['give_way'], r['junction'], r['no_exit'],
                    r['railway'], r['roundabout'], r['station'], r['stop'], r['traffic_calming'],
                    r['traffic_signal'], r['turning_loop'],
                    r['sunrise_sunset'], r['civil_twilight'], r['nautical_twilight'], r['astronomical_twilight']
                )
                for r in batch
            ]
            
            cur.executemany(insert_sql, rows)
        
        # Get statistics
        cur.execute("SELECT COUNT(*) FROM TRAFFIC_ACCIDENTS_HISTORICAL")
        total_count = cur.fetchone()[0]
        
        cur.execute("""
            SELECT 
                MIN(start_time) as earliest,
                MAX(start_time) as latest,
                COUNT(DISTINCT city) as cities,
                COUNT(DISTINCT state) as states
            FROM TRAFFIC_ACCIDENTS_HISTORICAL
        """)
        stats = cur.fetchone()
        
        cur.execute("COMMIT;")
       
        
    except Exception as e:
        try:
            cur.execute("ROLLBACK;")
        except Exception as rollback_error:
         raise
    finally:
        try:
            cur.close()
        except Exception:
            pass



with DAG(
    dag_id="Historical_Traffic_ETL",
    start_date=datetime(2025, 1, 1),
    description="ETL pipeline: Extract Kaggle traffic data (California), Transform, Load to Snowflake (Full Refresh)",
    schedule=None,  
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["ETL", "historical", "traffic", "full_refresh", "california"],
) as dag:

    # Extract from Kaggle
    extract_task = extract_kaggle_accidents()
    
    # Transform data
    transform_task = transform_accidents_data(extract_task)
    
    # Load to Snowflake
    load_task = load_to_snowflake(transform_task)
    
    # Set dependencies
    extract_task >> transform_task >> load_task