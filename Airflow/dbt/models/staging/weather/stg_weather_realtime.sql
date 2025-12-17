{{
    config(
        materialized='view',
        schema='STAGING',
        tags=['staging', 'weather', 'realtime']
    )
}}

-- FIXED: Use uppercase table name to match Snowflake convention
WITH raw_weather AS (
    SELECT * FROM {{ source('raw', 'WEATHER_CALIFORNIA_REALTIME_RAW') }}
),

cleaned AS (
    SELECT
        record_id,
        city,
        state,
        region,
        county,
        latitude,
        longitude,
        
        -- Parse timestamp
        CAST(timestamp_str AS TIMESTAMP_NTZ) AS timestamp_utc,
        date,
        hour,
        
        -- Temperature metrics
        temperature_f,
        feels_like_f,
        temp_min_f,
        temp_max_f,
        
        -- Temperature categories
        CASE
            WHEN temperature_f >= 90 THEN 'Hot'
            WHEN temperature_f >= 70 THEN 'Warm'
            WHEN temperature_f >= 50 THEN 'Mild'
            WHEN temperature_f >= 32 THEN 'Cold'
            ELSE 'Freezing'
        END AS temperature_category,
        
        -- Atmospheric metrics
        pressure_msl_hpa,
        surface_pressure_hpa,
        humidity_pct,
        
        -- Wind metrics
        wind_speed_mph,
        wind_direction_deg,
        wind_gusts_mph,
        
        -- Wind categories
        CASE
            WHEN wind_speed_mph >= 40 THEN 'High'
            WHEN wind_speed_mph >= 25 THEN 'Moderate'
            WHEN wind_speed_mph >= 10 THEN 'Light'
            ELSE 'Calm'
        END AS wind_category,
        
        -- Visibility and clouds
        cloud_cover_pct,
        
        -- Precipitation
        precipitation_in,
        rain_in,
        snowfall_in,
        
        -- Precipitation categories
        CASE
            WHEN precipitation_in >= 0.3 THEN 'Heavy'
            WHEN precipitation_in >= 0.1 THEN 'Moderate'
            WHEN precipitation_in > 0 THEN 'Light'
            ELSE 'None'
        END AS precipitation_category,
        
        -- Weather conditions
        weather_code,
        weather_main,
        weather_description,
        weather_icon,
        
        -- Flags for hazardous conditions
        CASE WHEN temperature_f <= 32 THEN TRUE ELSE FALSE END AS is_freezing,
        CASE WHEN wind_speed_mph >= 40 THEN TRUE ELSE FALSE END AS is_high_wind,
        CASE WHEN precipitation_in >= 0.3 THEN TRUE ELSE FALSE END AS is_heavy_rain,
        CASE WHEN snowfall_in > 0 THEN TRUE ELSE FALSE END AS has_snow,
        
        -- Metadata
        raw_json,
        load_timestamp,
        data_source
        
    FROM raw_weather
    WHERE temperature_f IS NOT NULL
        AND latitude BETWEEN -90 AND 90
        AND longitude BETWEEN -180 AND 180
)

SELECT * FROM cleaned