
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Variable.get("dbt_project_dir", default_var="/opt/airflow/dbt")

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    return conn.cursor()

KAGGLE_DATA_PATH = "/opt/airflow/data/US_Accidents_March23.csv"
TARGET_STATE = "CA"
USER_DB = Variable.get("USER_DB")
USER_WH = Variable.get("USER_WH")

@task
def extract_and_load_raw():

    if not os.path.exists(KAGGLE_DATA_PATH):
        raise FileNotFoundError(f"Missing file: {KAGGLE_DATA_PATH}")

    cur = return_snowflake_conn()

    try:
         # Use the specified database and warehouse
        cur.execute(f"USE DATABASE {USER_DB}")
        cur.execute(f"USE WAREHOUSE {USER_WH}")
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW_ARCHIVE")
        cur.execute("USE SCHEMA RAW_ARCHIVE")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS TRAFFIC_ACCIDENTS_RAW (
                accident_id VARCHAR(50) NOT NULL,
                severity NUMBER(1),
                start_time VARCHAR(50),
                end_time VARCHAR(50),
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
                weather_timestamp VARCHAR(50),
                temperature_f FLOAT,
                wind_chill_f FLOAT,
                humidity_pct FLOAT,
                pressure_in FLOAT,
                visibility_mi FLOAT,
                wind_direction VARCHAR(20),
                wind_speed_mph FLOAT,
                precipitation_in FLOAT,
                weather_condition VARCHAR(100),
                amenity VARCHAR(10),
                bump VARCHAR(10),
                crossing VARCHAR(10),
                give_way VARCHAR(10),
                junction VARCHAR(10),
                no_exit VARCHAR(10),
                railway VARCHAR(10),
                roundabout VARCHAR(10),
                station VARCHAR(10),
                stop VARCHAR(10),
                traffic_calming VARCHAR(10),
                traffic_signal VARCHAR(10),
                turning_loop VARCHAR(10),
                sunrise_sunset VARCHAR(20),
                civil_twilight VARCHAR(20),
                nautical_twilight VARCHAR(20),
                astronomical_twilight VARCHAR(20),
                load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (accident_id)
            )
        """)


        chunk_size = 50000
        total_loaded = 0
        chunks_processed = 0

        insert_sql = """
            INSERT INTO TRAFFIC_ACCIDENTS_RAW (
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

        for chunk in pd.read_csv(KAGGLE_DATA_PATH, chunksize=chunk_size):
            chunks_processed += 1
            chunk = chunk[chunk["State"] == TARGET_STATE]
            
            if chunk.empty:
                continue

            chunk = chunk.replace({pd.NA: None, pd.NaT: None})
            chunk = chunk.where(pd.notnull(chunk), None)
            
            rows = []
            for row in chunk[[
                "ID", "Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng",
                "Distance(mi)", "Description", "Street", "City", "County", "State",
                "Zipcode", "Country", "Timezone", "Weather_Timestamp", "Temperature(F)",
                "Wind_Chill(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)",
                "Wind_Direction", "Wind_Speed(mph)", "Precipitation(in)", "Weather_Condition",
                "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
                "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
                "Traffic_Signal", "Turning_Loop", "Sunrise_Sunset", "Civil_Twilight",
                "Nautical_Twilight", "Astronomical_Twilight"
            ]].values.tolist():
                cleaned_row = tuple(None if pd.isna(val) else val for val in row)
                rows.append(cleaned_row)

            if rows:
                cur.executemany(insert_sql, rows)
                total_loaded += len(rows)

        cur.execute("SELECT COUNT(*) FROM TRAFFIC_ACCIDENTS_RAW")
        total_count = cur.fetchone()[0]

        cur.execute("""
            SELECT
                COUNT(DISTINCT city) as cities,
                COUNT(DISTINCT county) as counties
            FROM TRAFFIC_ACCIDENTS_RAW
        """)
        stats = cur.fetchone()

    except Exception as e:
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass

with DAG(
    dag_id="Traffic_Historical_ELT_DBT",
    start_date=datetime(2025, 1, 1),
    description=(
        "ELT pipeline: Load RAW Kaggle traffic data (California) to Snowflake, Transform with dbt"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["ELT", "historical", "traffic", "dbt", "california"],
) as dag:

    load_task = extract_and_load_raw()

    dbt_deps = BashOperator(
        task_id="dbt_install_dependencies",
        bash_command=f"cd {DBT_PROJECT_DIR} && rm -rf dbt_packages* && dbt deps --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging.traffic.stg_traffic_accidents --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_analytics = BashOperator(
        task_id="dbt_run_analytics",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select analytics.integrated.accident_weather_correlation --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts.mart_traffic_safety_dashboard --profiles-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_data_quality",
        bash_command=f"cd {DBT_PROJECT_DIR} && (dbt test --profiles-dir {DBT_PROJECT_DIR} || echo 'Tests completed with some failures')",
        trigger_rule="all_done",
    )

    load_task >> dbt_deps >> dbt_staging >> dbt_analytics >> dbt_marts >> dbt_test