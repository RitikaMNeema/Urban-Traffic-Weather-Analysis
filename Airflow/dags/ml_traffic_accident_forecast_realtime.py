
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.models import Variable

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
USER_DB = Variable.get("USER_DB")
USER_WH = Variable.get("USER_WH")

def get_snowflake_cursor():
    """Get Snowflake cursor using Airflow connection."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    return hook.get_conn().cursor()

@task
def prepare_enriched_training_data(train_input_table, train_view):
   

    cur = get_snowflake_cursor()

    try:
        cur.execute(f"USE DATABASE {USER_DB}")
        cur.execute(f"USE WAREHOUSE {USER_WH}")
        cur.execute("CREATE SCHEMA IF NOT EXISTS ML_PREDICTIONS")
        cur.execute("USE SCHEMA ML_PREDICTIONS")

        logger.info("Creating enriched training data table...")

        # Use 2 years (~730 days) of history for accidents + weather
        create_training_table = f"""
        CREATE OR REPLACE TABLE {train_input_table} AS
        WITH accident_daily AS (
            SELECT
                DATE_TRUNC('DAY', start_time) AS date,
                city,
                COUNT(*) AS accident_count,
                AVG(severity) AS avg_severity,
                COUNT(CASE WHEN severity >= 3 THEN 1 END) AS severe_accident_count
            FROM RAW_ARCHIVE.TRAFFIC_ACCIDENTS_HISTORICAL
            WHERE start_time IS NOT NULL
              AND start_time >= DATEADD('day', -1000, CURRENT_DATE())
              AND city IS NOT NULL
            GROUP BY DATE_TRUNC('DAY', start_time), city
        ),
        weather_daily AS (
            SELECT
                DATE_TRUNC('DAY', timestamp_utc) AS date,
                city,
                AVG(temperature_f) AS avg_temp_f,
                AVG(relative_humidity_pct) AS avg_humidity_pct,
                AVG(wind_speed_mph) AS avg_wind_speed_mph,
                AVG(wind_gusts_mph) AS avg_wind_gusts_mph,
                SUM(precipitation_mm) AS total_precipitation_mm,
                SUM(rain_mm) AS total_rain_mm,
                SUM(snowfall_mm) AS total_snowfall_mm,
                AVG(cloud_cover_pct) AS avg_cloud_cover_pct,
                AVG(pressure_msl_hpa) AS avg_pressure_hpa,
                MAX(wind_speed_mph) AS max_wind_speed_mph,
                MIN(temperature_f) AS min_temp_f,
                MAX(temperature_f) AS max_temp_f
            FROM RAW_ARCHIVE.WEATHER_CALIFORNIA_HISTORICAL_RAW
            WHERE timestamp_utc IS NOT NULL
              AND timestamp_utc >= DATEADD('day', -1000, CURRENT_DATE())
              AND city IS NOT NULL
            GROUP BY DATE_TRUNC('DAY', timestamp_utc), city
        )
        SELECT
            a.date,
            a.city,
            a.accident_count,
            a.avg_severity,
            a.severe_accident_count,
            -- Weather features
            COALESCE(w.avg_temp_f, 65.0) AS avg_temp_f,
            COALESCE(w.avg_humidity_pct, 50.0) AS avg_humidity_pct,
            COALESCE(w.avg_wind_speed_mph, 5.0) AS avg_wind_speed_mph,
            COALESCE(w.avg_wind_gusts_mph, 10.0) AS avg_wind_gusts_mph,
            COALESCE(w.total_precipitation_mm, 0.0) AS total_precipitation_mm,
            COALESCE(w.total_rain_mm, 0.0) AS total_rain_mm,
            COALESCE(w.total_snowfall_mm, 0.0) AS total_snowfall_mm,
            COALESCE(w.avg_cloud_cover_pct, 30.0) AS avg_cloud_cover_pct,
            COALESCE(w.avg_pressure_hpa, 1013.0) AS avg_pressure_hpa,
            COALESCE(w.max_wind_speed_mph, 10.0) AS max_wind_speed_mph,
            COALESCE(w.min_temp_f, 60.0) AS min_temp_f,
            COALESCE(w.max_temp_f, 75.0) AS max_temp_f,
            -- Temporal features
            DAYOFWEEK(a.date) AS day_of_week,
            CASE WHEN DAYOFWEEK(a.date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
            MONTH(a.date) AS month,
            QUARTER(a.date) AS quarter,
            -- Weather risk factors
            CASE WHEN COALESCE(w.total_precipitation_mm, 0) > 10 THEN 1 ELSE 0 END AS heavy_precipitation,
            CASE WHEN COALESCE(w.max_wind_speed_mph, 0) > 25 THEN 1 ELSE 0 END AS high_wind,
            CASE WHEN COALESCE(w.avg_temp_f, 65) < 32 THEN 1 ELSE 0 END AS freezing_temp,
            CASE WHEN COALESCE(w.total_snowfall_mm, 0) > 0 THEN 1 ELSE 0 END AS snow_present
        FROM accident_daily a
        LEFT JOIN weather_daily w
          ON a.date = w.date
         AND a.city = w.city
        WHERE a.accident_count >= 3
        ORDER BY a.city, a.date
        """
        cur.execute(create_training_table)

        # Check if we have data
        cur.execute(f"SELECT COUNT(*) FROM {train_input_table}")
        total_records = cur.fetchone()[0]

        if total_records == 0:
            
            raise ValueError("No training data available")

        # Stats
        cur.execute(f"""
        SELECT
            COUNT(DISTINCT date) as days,
            COUNT(DISTINCT city) as cities,
            MIN(date) as start_date,
            MAX(date) as end_date,
            SUM(accident_count) as total_accidents,
            AVG(avg_temp_f) as avg_temperature,
            SUM(total_precipitation_mm) as total_precipitation
        FROM {train_input_table}
        """)
        stats = cur.fetchone()

        

        # Training view (2 years)
        logger.info("\nCreating training view (last 2 years)...")
        create_view = f"""
        CREATE OR REPLACE VIEW {train_view} AS
        SELECT
            date,
            city,
            accident_count,
            avg_temp_f,
            avg_humidity_pct,
            avg_wind_speed_mph,
            total_precipitation_mm,
            total_rain_mm,
            total_snowfall_mm,
            day_of_week,
            is_weekend,
            month,
            heavy_precipitation,
            high_wind,
            freezing_temp,
            snow_present
        FROM {train_input_table}
        WHERE date >= DATEADD('day', -1000, CURRENT_DATE())
        ORDER BY city, date
        """
        cur.execute(create_view)

        logger.info("✅ Training view created")
        logger.info("=" * 80)

    except Exception as e:
        raise
    finally:
        cur.close()

@task
def train_model_with_features(train_view, forecast_function_name):
    """Train Snowflake ML.FORECAST model with weather + temporal features."""
    logger.info("=" * 80)
    logger.info("TRAINING ML MODEL WITH WEATHER + TEMPORAL FEATURES")
    logger.info("=" * 80)

    cur = get_snowflake_cursor()

    try:
        cur.execute(f"USE DATABASE {USER_DB}")
        cur.execute(f"USE WAREHOUSE {USER_WH}")
        cur.execute("USE SCHEMA ML_PREDICTIONS")

        logger.info(f"Training model: {forecast_function_name}")
        logger.info("Features: weather conditions, temporal patterns, risk factors")
        logger.info("This may take 10-20 minutes...")

        create_model = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA        => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME    => 'CITY',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME    => 'ACCIDENT_COUNT',
            CONFIG_OBJECT     => {{ 'ON_ERROR': 'SKIP' }}
        )
        """
        cur.execute(create_model)


    except Exception as e:
        raise
    finally:
        cur.close()

@task
def generate_realtime_forecast(forecast_function_name, train_view, train_input_table,
                               forecast_table, final_table):
    """
    Generate 7-day predictions using the trained model.
    Creates future exogenous data and passes it to the FORECAST function.
    """
    logger.info("=" * 80)
    logger.info("GENERATING 7-DAY FORECAST WITH REAL-TIME CONDITIONS")
    logger.info("=" * 80)

    cur = get_snowflake_cursor()

    try:
        cur.execute(f"USE DATABASE {USER_DB}")
        cur.execute(f"USE WAREHOUSE {USER_WH}")
        cur.execute("USE SCHEMA ML_PREDICTIONS")

        # Step 1: Create future exogenous data table with predicted weather/temporal features
        logger.info("Creating future exogenous data for 7-day forecast...")
        
        create_future_data = """
        CREATE OR REPLACE TABLE ML_PREDICTIONS.FUTURE_EXOGENOUS_DATA AS
        WITH date_series AS (
            SELECT
                DATEADD('day', seq4(), CURRENT_DATE()) AS date
            FROM TABLE(GENERATOR(ROWCOUNT => 7))
        ),
        cities AS (
            SELECT DISTINCT city
            FROM USER_DB_JACKAL.ML_PREDICTIONS.ACCIDENT_TRAINING_VIEW_ENRICHED
        ),
        future_dates AS (
            SELECT
                d.date,
                c.city
            FROM date_series d
            CROSS JOIN cities c
        ),
        latest_weather AS (
            SELECT
                city,
                AVG(temperature_f) AS current_temp_f,
                AVG(humidity_pct) AS current_humidity_pct,
                AVG(wind_speed_mph) AS current_wind_speed_mph,
                AVG(precipitation_in) * 25.4 AS current_precipitation_mm,
                AVG(cloud_cover_pct) AS current_cloud_cover_pct,
                MAX(CASE WHEN wind_speed_mph > 25 THEN 1 ELSE 0 END) AS has_high_wind,
                MAX(CASE WHEN temperature_f < 32 THEN 1 ELSE 0 END) AS has_freezing_temp,
                MAX(CASE WHEN precipitation_in > 0.04 THEN 1 ELSE 0 END) AS has_precipitation
            FROM RAW.WEATHER_CALIFORNIA_REALTIME_RAW
            WHERE timestamp_str >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
            GROUP BY city
        )
        SELECT
            f.date,
            f.city,
            -- Use recent weather as proxy for future (matches training view schema)
            COALESCE(w.current_temp_f, 65.0) AS avg_temp_f,
            COALESCE(w.current_humidity_pct, 50.0) AS avg_humidity_pct,
            COALESCE(w.current_wind_speed_mph, 5.0) AS avg_wind_speed_mph,
            COALESCE(w.current_precipitation_mm, 0.0) AS total_precipitation_mm,
            COALESCE(w.current_precipitation_mm, 0.0) AS total_rain_mm,
            0.0 AS total_snowfall_mm,
            -- Temporal features (same as training)
            DAYOFWEEK(f.date) AS day_of_week,
            CASE WHEN DAYOFWEEK(f.date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
            MONTH(f.date) AS month,
            -- Weather risk factors (same as training)
            COALESCE(w.has_precipitation, 0) AS heavy_precipitation,
            COALESCE(w.has_high_wind, 0) AS high_wind,
            COALESCE(w.has_freezing_temp, 0) AS freezing_temp,
            0 AS snow_present
        FROM future_dates f
        LEFT JOIN latest_weather w ON f.city = w.city
        ORDER BY f.city, f.date
        """
        cur.execute(create_future_data)
        
        # Verify data was created
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT city), MIN(date), MAX(date) FROM ML_PREDICTIONS.FUTURE_EXOGENOUS_DATA")
        stats = cur.fetchone()

        # Step 2: Generate forecast using INPUT_DATA with the future exogenous data
        
        # For multi-series with exogenous variables, must specify all three parameters
        make_forecast = f"""
        BEGIN
            CALL {forecast_function_name}!FORECAST(
                INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'ML_PREDICTIONS.FUTURE_EXOGENOUS_DATA'),
                SERIES_COLNAME => 'CITY',
                TIMESTAMP_COLNAME => 'DATE'
            );
            LET x := SQLID;
            CREATE OR REPLACE TABLE {forecast_table} AS
            SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;
        """
        cur.execute(make_forecast)
        
        # Check forecast results
        cur.execute(f"SELECT COUNT(*) FROM {forecast_table}")
        forecast_count = cur.fetchone()[0]

        # Step 3: Enrich with real-time conditions
        logger.info("Enriching with real-time weather and traffic data...")
        create_final = f"""
        CREATE OR REPLACE TABLE {final_table} AS
        WITH latest_weather AS (
            SELECT
                city,
                AVG(temperature_f) AS current_temp_f,
                AVG(humidity_pct) AS current_humidity_pct,
                AVG(wind_speed_mph) AS current_wind_speed_mph,
                SUM(precipitation_in) AS current_precipitation_in,
                AVG(cloud_cover_pct) AS current_cloud_cover_pct,
                MAX(CASE WHEN precipitation_in > 0.1 THEN 1 ELSE 0 END) AS has_precipitation,
                MAX(CASE WHEN wind_speed_mph > 25 THEN 1 ELSE 0 END) AS has_high_wind,
                MAX(CASE WHEN temperature_f < 32 THEN 1 ELSE 0 END) AS has_freezing_temp
            FROM RAW.WEATHER_CALIFORNIA_REALTIME_RAW
            WHERE timestamp_str >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
            GROUP BY city
        ),
        latest_traffic AS (
            SELECT
                city,
                AVG(current_speed) AS avg_current_speed,
                AVG(free_flow_speed) AS avg_free_flow_speed,
                AVG(
                    CASE
                        WHEN free_flow_speed > 0
                        THEN (free_flow_speed - current_speed) / free_flow_speed * 100
                        ELSE 0
                    END
                ) AS avg_congestion_pct,
                COUNT(*) AS traffic_data_points
            FROM RAW.TRAFFIC_FLOW_RAW
            WHERE timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
            GROUP BY city
        ),
        historical_actuals AS (
            SELECT
                city,
                date,
                accident_count AS actual_accident_count,
                NULL AS forecasted_accident_count,
                NULL AS lower_bound,
                NULL AS upper_bound,
                'ACTUAL' AS data_type,
                avg_temp_f,
                total_precipitation_mm,
                avg_wind_speed_mph,
                NULL AS current_temp_f,
                NULL AS current_congestion_pct,
                NULL AS risk_multiplier
            FROM {train_input_table}
            WHERE date >= DATEADD('day', -30, CURRENT_DATE())
        ),
        forecasted_data AS (
            SELECT
                REPLACE(f.series, '"', '') AS city,
                CAST(f.ts AS DATE) AS date,
                NULL AS actual_accident_count,
                ROUND(f.forecast, 2) AS forecasted_accident_count,
                ROUND(f.lower_bound, 2) AS lower_bound,
                ROUND(f.upper_bound, 2) AS upper_bound,
                'FORECAST' AS data_type,
                NULL AS avg_temp_f,
                NULL AS total_precipitation_mm,
                NULL AS avg_wind_speed_mph,
                w.current_temp_f,
                t.avg_congestion_pct AS current_congestion_pct,
                (
                    1.0
                    + COALESCE(w.has_precipitation * 0.15, 0)
                    + COALESCE(w.has_high_wind * 0.10, 0)
                    + COALESCE(w.has_freezing_temp * 0.20, 0)
                    + CASE WHEN COALESCE(t.avg_congestion_pct, 0) > 30 THEN 0.12 ELSE 0 END
                ) AS risk_multiplier
            FROM {forecast_table} f
            LEFT JOIN latest_weather w ON REPLACE(f.series, '"', '') = w.city
            LEFT JOIN latest_traffic t ON REPLACE(f.series, '"', '') = t.city
        )
        SELECT
            city,
            date,
            actual_accident_count,
            CASE
                WHEN data_type = 'FORECAST' AND risk_multiplier IS NOT NULL
                    THEN ROUND(forecasted_accident_count * risk_multiplier, 2)
                ELSE forecasted_accident_count
            END AS forecasted_accident_count,
            CASE
                WHEN data_type = 'FORECAST' AND risk_multiplier IS NOT NULL
                    THEN ROUND(lower_bound * risk_multiplier, 2)
                ELSE lower_bound
            END AS lower_bound,
            CASE
                WHEN data_type = 'FORECAST' AND risk_multiplier IS NOT NULL
                    THEN ROUND(upper_bound * risk_multiplier, 2)
                ELSE upper_bound
            END AS upper_bound,
            data_type,
            avg_temp_f,
            total_precipitation_mm,
            avg_wind_speed_mph,
            current_temp_f,
            current_congestion_pct,
            risk_multiplier
        FROM historical_actuals
        UNION ALL
        SELECT * FROM forecasted_data
        ORDER BY city, date
        """
        cur.execute(create_final)

        # Statistics and reporting
        cur.execute(f"""
        SELECT
            data_type,
            COUNT(*) as records,
            COUNT(DISTINCT city) as cities,
            MIN(date) as start_date,
            MAX(date) as end_date,
            AVG(CASE WHEN data_type = 'FORECAST' THEN risk_multiplier END) as avg_risk_multiplier
        FROM {final_table}
        GROUP BY data_type
        """)
     
        for row in cur.fetchall():
            logger.info(f"{row[0]}: {row[1]:,} records | {row[2]} cities | {row[3]} to {row[4]}")
            if row[5]:
                logger.info(f"  Avg Risk Multiplier: {row[5]:.2f}x")

        cur.execute(f"""
        SELECT
            city,
            date,
            forecasted_accident_count,
            current_temp_f,
            current_congestion_pct,
            risk_multiplier
        FROM {final_table}
        WHERE data_type = 'FORECAST'
        ORDER BY forecasted_accident_count DESC
        LIMIT 10
        """)
        
        for row in cur.fetchall():
            temp = f"{row[3]:.1f}°F" if row[3] else "N/A"
            traffic = f"{row[4]:.0f}%" if row[4] else "N/A"
            risk = f"{row[5]:.2f}x" if row[5] else "1.00x"
            logger.info(f"{row[0]:<20} {str(row[1]):<12} {row[2]:<12.1f} {temp:<8} {traffic:<10} {risk:<8}")
        
        logger.info("\n" + "=" * 80)
        logger.info(f"Query predictions: SELECT * FROM {final_table} WHERE data_type = 'FORECAST'")
        logger.info("=" * 80)

    except Exception as e:
        raise
    finally:
        cur.close()

@task
def create_comprehensive_risk_view(final_table):
    

    cur = get_snowflake_cursor()

    try:
        cur.execute(f"USE DATABASE {USER_DB}")
        cur.execute("USE SCHEMA ML_PREDICTIONS")

        create_view = f"""
        CREATE OR REPLACE VIEW ACCIDENT_RISK_ASSESSMENT_REALTIME AS
        SELECT
            city,
            date,
            forecasted_accident_count,
            lower_bound,
            upper_bound,
            current_temp_f,
            current_congestion_pct,
            risk_multiplier,
            CASE
                WHEN forecasted_accident_count >= 50 THEN 'CRITICAL'
                WHEN forecasted_accident_count >= 30 THEN 'HIGH'
                WHEN forecasted_accident_count >= 15 THEN 'MEDIUM'
                ELSE 'LOW'
            END AS risk_level,
            CASE
                WHEN current_temp_f IS NOT NULL AND current_temp_f < 32 THEN 'FREEZING'
                WHEN current_temp_f IS NOT NULL AND current_temp_f > 95 THEN 'EXTREME_HEAT'
                ELSE 'NORMAL'
            END AS temperature_risk,
            CASE
                WHEN current_congestion_pct IS NOT NULL AND current_congestion_pct > 40 THEN 'SEVERE_CONGESTION'
                WHEN current_congestion_pct IS NOT NULL AND current_congestion_pct > 20 THEN 'MODERATE_CONGESTION'
                ELSE 'NORMAL_FLOW'
            END AS traffic_risk,
            DAYNAME(date) AS day_of_week,
            CASE
                WHEN DAYNAME(date) IN ('Sat', 'Sun') THEN TRUE
                ELSE FALSE
            END AS is_weekend,
            LEAST(
                100,
                ROUND(
                    (forecasted_accident_count / 50.0 * 50)
                    + (COALESCE(risk_multiplier - 1, 0) * 50),
                    0
                )
            ) AS combined_risk_score
        FROM {final_table}
        WHERE data_type = 'FORECAST'
        ORDER BY forecasted_accident_count DESC
        """
        cur.execute(create_view)


    except Exception as e:
        raise
    finally:
        cur.close()

with DAG(
    dag_id="ML_Traffic_Accident_Forecast_Realtime",
    start_date=datetime(2025, 1, 1),
    description="ML forecasting with real-time weather + traffic: Predict 7-day accident trends using current conditions",
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=5),
    tags=["ML", "forecasting", "traffic", "weather", "realtime", "snowflake-ml"],
) as dag:

    train_input_table = Variable.get(
        "traffic_train_input_table_realtime",
        default_var=f"{USER_DB}.ML_PREDICTIONS.ACCIDENT_TRAINING_DATA_ENRICHED",
    )

    train_view = Variable.get(
        "traffic_train_view_realtime",
        default_var=f"{USER_DB}.ML_PREDICTIONS.ACCIDENT_TRAINING_VIEW_ENRICHED",
    )

    forecast_table = Variable.get(
        "traffic_forecast_table_realtime",
        default_var=f"{USER_DB}.ML_PREDICTIONS.ACCIDENT_FORECAST_RAW_REALTIME",
    )

    forecast_function_name = Variable.get(
        "traffic_forecast_function_realtime",
        default_var=f"{USER_DB}.ML_PREDICTIONS.ACCIDENT_FORECASTER_REALTIME",
    )

    final_table = Variable.get(
        "traffic_final_table_realtime",
        default_var=f"{USER_DB}.ML_PREDICTIONS.ACCIDENT_FORECAST_FINAL_REALTIME",
    )

    prep = prepare_enriched_training_data(train_input_table, train_view)
    train = train_model_with_features(train_view, forecast_function_name)
    forecast = generate_realtime_forecast(
        forecast_function_name, train_view, train_input_table, forecast_table, final_table
    )
    risk_view = create_comprehensive_risk_view(final_table)

    prep >> train >> forecast >> risk_view
