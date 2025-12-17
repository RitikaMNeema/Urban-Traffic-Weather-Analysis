-- stg_weather_historical.sql (Open-Meteo API compatible)

{{ config(
    materialized='view',
    tags=['staging', 'weather', 'historical']
) }}

WITH source AS (
    SELECT * FROM {{ source('raw_archive', 'WEATHER_CALIFORNIA_HISTORICAL_RAW') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        record_id,

        -- Location
        city,
        state,
        region,
        county,
        'US' AS country,
        latitude,
        longitude,

        -- Temporal - Open-Meteo provides timestamp_utc as TIMESTAMP_NTZ
        timestamp_utc AS timestamp,
        TO_DATE(timestamp_utc) AS date,
        DATE_PART('HOUR', timestamp_utc) AS hour,
        DATE_PART('DOW', TO_DATE(timestamp_utc)) AS day_of_week,
        TO_CHAR(TO_DATE(timestamp_utc), 'DAY') AS day_name,
        CASE
            WHEN DATE_PART('DOW', TO_DATE(timestamp_utc)) IN (0, 6)
                THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        -- Temperature metrics (already in Fahrenheit from Open-Meteo)
        temperature_f,
        temperature_f AS feels_like_f,
        temperature_f AS temp_min_f,
        temperature_f AS temp_max_f,

        -- Atmospheric conditions - convert units where needed
        ROUND(pressure_msl_hpa * 0.02953, 2) AS pressure_in,
        relative_humidity_pct AS humidity_pct,
        NULL AS visibility_mi,
        cloud_cover_pct AS clouds_pct,

        -- Wind conditions (already in MPH from Open-Meteo)
        wind_speed_mph,
        wind_direction_deg AS wind_deg,
        wind_gusts_mph AS wind_gust_mph,

        -- Precipitation - convert units (mm -> in)
        ROUND(rain_mm * 0.0393701, 3) AS rain_1h_in,
        NULL AS rain_3h_in,
        ROUND(snowfall_mm * 0.0393701, 3) AS snow_1h_in,
        NULL AS snow_3h_in,
        ROUND(precipitation_mm * 0.0393701, 3) AS total_precip_1h_in,

        -- Weather classification - Convert WMO weather codes to descriptions
        weather_code,
        CASE
            WHEN weather_code = 0 THEN 'Clear'
            WHEN weather_code IN (1, 2, 3) THEN 'Clouds'
            WHEN weather_code IN (45, 48) THEN 'Fog'
            WHEN weather_code IN (51, 53, 55, 61, 63, 65, 80, 81, 82) THEN 'Rain'
            WHEN weather_code IN (66, 67) THEN 'Freezing Rain'
            WHEN weather_code IN (71, 73, 75, 77, 85, 86) THEN 'Snow'
            WHEN weather_code IN (95, 96, 99) THEN 'Thunderstorm'
            ELSE 'Unknown'
        END AS weather_main,
        
        CASE
            WHEN weather_code = 0 THEN 'clear sky'
            WHEN weather_code = 1 THEN 'mainly clear'
            WHEN weather_code = 2 THEN 'partly cloudy'
            WHEN weather_code = 3 THEN 'overcast'
            WHEN weather_code = 45 THEN 'fog'
            WHEN weather_code = 48 THEN 'depositing rime fog'
            WHEN weather_code = 51 THEN 'light drizzle'
            WHEN weather_code = 53 THEN 'moderate drizzle'
            WHEN weather_code = 55 THEN 'dense drizzle'
            WHEN weather_code = 61 THEN 'slight rain'
            WHEN weather_code = 63 THEN 'moderate rain'
            WHEN weather_code = 65 THEN 'heavy rain'
            WHEN weather_code = 66 THEN 'light freezing rain'
            WHEN weather_code = 67 THEN 'heavy freezing rain'
            WHEN weather_code = 71 THEN 'slight snow'
            WHEN weather_code = 73 THEN 'moderate snow'
            WHEN weather_code = 75 THEN 'heavy snow'
            WHEN weather_code = 77 THEN 'snow grains'
            WHEN weather_code = 80 THEN 'slight rain showers'
            WHEN weather_code = 81 THEN 'moderate rain showers'
            WHEN weather_code = 82 THEN 'violent rain showers'
            WHEN weather_code = 85 THEN 'slight snow showers'
            WHEN weather_code = 86 THEN 'heavy snow showers'
            WHEN weather_code = 95 THEN 'thunderstorm'
            WHEN weather_code = 96 THEN 'thunderstorm with slight hail'
            WHEN weather_code = 99 THEN 'thunderstorm with heavy hail'
            ELSE 'unknown'
        END AS weather_description,
        
        CASE
            WHEN weather_code = 0 THEN '01d'
            WHEN weather_code IN (1, 2) THEN '02d'
            WHEN weather_code = 3 THEN '03d'
            WHEN weather_code IN (45, 48) THEN '50d'
            WHEN weather_code IN (51, 53, 55) THEN '09d'
            WHEN weather_code IN (61, 63, 65, 80, 81, 82) THEN '10d'
            WHEN weather_code IN (71, 73, 75, 77, 85, 86) THEN '13d'
            WHEN weather_code IN (95, 96, 99) THEN '11d'
            ELSE '01d'
        END AS weather_icon,

        -- Derived categories (temperature)
        CASE
            WHEN temperature_f < 32 THEN 'Freezing'
            WHEN temperature_f < 50 THEN 'Cold'
            WHEN temperature_f < 70 THEN 'Moderate'
            WHEN temperature_f < 85 THEN 'Warm'
            ELSE 'Hot'
        END AS temperature_category,

        -- Derived categories (visibility)
        'Unknown' AS visibility_category,

        -- Derived categories (wind)
        CASE
            WHEN wind_speed_mph < 5  THEN 'Calm'
            WHEN wind_speed_mph < 15 THEN 'Light'
            WHEN wind_speed_mph < 25 THEN 'Moderate'
            WHEN wind_speed_mph < 35 THEN 'Strong'
            ELSE 'Very Strong'
        END AS wind_category,

        -- Derived categories (precipitation)
        CASE
            WHEN COALESCE(precipitation_mm, 0) * 0.0393701 = 0 THEN 'None'
            WHEN COALESCE(precipitation_mm, 0) * 0.0393701 < 0.1 THEN 'Light'
            WHEN COALESCE(precipitation_mm, 0) * 0.0393701 < 0.3 THEN 'Moderate'
            ELSE 'Heavy'
        END AS precipitation_category,

        -- Adverse conditions flags
        CASE WHEN temperature_f < 32 THEN TRUE ELSE FALSE END AS is_freezing,
        FALSE AS is_poor_visibility,
        CASE WHEN wind_speed_mph > 25 THEN TRUE ELSE FALSE END AS is_high_wind,
        CASE WHEN COALESCE(rain_mm, 0) * 0.0393701 > 0.1 THEN TRUE ELSE FALSE END AS is_heavy_rain,
        CASE WHEN COALESCE(snowfall_mm, 0) > 0 THEN TRUE ELSE FALSE END AS has_snow,

        -- Metadata
        load_timestamp,
        data_source,
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM source
    WHERE
        record_id IS NOT NULL
        AND timestamp_utc IS NOT NULL
        AND state = 'CA'
)

SELECT * FROM cleaned