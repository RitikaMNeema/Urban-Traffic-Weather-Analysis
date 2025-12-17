-- stg_traffic_accidents.sql
-- Path: /opt/airflow/dbt/models/staging/traffic/stg_traffic_accidents.sql

{{ config(
    materialized = 'view',
    tags = ['staging', 'traffic', 'historical']
) }}

WITH source AS (

    SELECT * FROM {{ source('raw_archive', 'TRAFFIC_ACCIDENTS_RAW') }}

),

cleaned AS (

    SELECT

        -- Identifiers
        accident_id,

        -- Classification
        CAST(severity AS INTEGER) AS severity,
        CASE
            WHEN severity = 1 THEN 'Minor'
            WHEN severity = 2 THEN 'Moderate'
            WHEN severity = 3 THEN 'Serious'
            WHEN severity = 4 THEN 'Severe'
            ELSE 'Unknown'
        END AS severity_label,

        -- Temporal
        TRY_CAST(start_time AS TIMESTAMP_NTZ) AS start_time,
        TRY_CAST(end_time AS TIMESTAMP_NTZ) AS end_time,
        DATE(TRY_CAST(start_time AS TIMESTAMP_NTZ)) AS accident_date,
        HOUR(TRY_CAST(start_time AS TIMESTAMP_NTZ)) AS accident_hour,
        DAYOFWEEK(TRY_CAST(start_time AS TIMESTAMP_NTZ)) AS day_of_week,
        DAYNAME(TRY_CAST(start_time AS TIMESTAMP_NTZ)) AS day_name,
        CASE
            WHEN DAYOFWEEK(TRY_CAST(start_time AS TIMESTAMP_NTZ)) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        -- Location
        CAST(start_lat AS FLOAT) AS latitude,
        CAST(start_lng AS FLOAT) AS longitude,
        COALESCE(NULLIF(TRIM(street), ''), 'Unknown') AS street,
        COALESCE(NULLIF(TRIM(city), ''), 'Unknown') AS city,
        COALESCE(NULLIF(TRIM(county), ''), 'Unknown') AS county,
        COALESCE(NULLIF(TRIM(state), ''), 'CA') AS state,
        COALESCE(NULLIF(TRIM(zipcode), ''), 'Unknown') AS zipcode,
        COALESCE(NULLIF(TRIM(timezone), ''), 'America/Los_Angeles') AS timezone,

        -- Impact
        CAST(distance_mi AS FLOAT) AS distance_affected_mi,
        COALESCE(NULLIF(TRIM(description), ''), 'No description') AS description,

        -- Weather conditions at accident time
        TRY_CAST(weather_timestamp AS TIMESTAMP_NTZ) AS weather_timestamp,
        CAST(temperature_f AS FLOAT) AS temperature_f,
        CAST(wind_chill_f AS FLOAT) AS wind_chill_f,
        CAST(humidity_pct AS FLOAT) AS humidity_pct,
        CAST(pressure_in AS FLOAT) AS pressure_in,
        CAST(visibility_mi AS FLOAT) AS visibility_mi,
        COALESCE(NULLIF(TRIM(wind_direction), ''), 'Unknown') AS wind_direction,
        CAST(wind_speed_mph AS FLOAT) AS wind_speed_mph,
        CAST(precipitation_in AS FLOAT) AS precipitation_in,
        COALESCE(NULLIF(TRIM(weather_condition), ''), 'Clear') AS weather_condition,

        -- Road features (convert string 'True'/'False' to boolean)
        CASE WHEN UPPER(TRIM(amenity)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_amenity,
        CASE WHEN UPPER(TRIM(bump)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_bump,
        CASE WHEN UPPER(TRIM(crossing)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_crossing,
        CASE WHEN UPPER(TRIM(give_way)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_give_way,
        CASE WHEN UPPER(TRIM(junction)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_junction,
        CASE WHEN UPPER(TRIM(no_exit)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_no_exit,
        CASE WHEN UPPER(TRIM(railway)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_railway,
        CASE WHEN UPPER(TRIM(roundabout)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_roundabout,
        CASE WHEN UPPER(TRIM(station)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_station,
        CASE WHEN UPPER(TRIM(stop)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_stop,
        CASE WHEN UPPER(TRIM(traffic_calming)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_traffic_calming,
        CASE WHEN UPPER(TRIM(traffic_signal)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_traffic_signal,
        CASE WHEN UPPER(TRIM(turning_loop)) = 'TRUE' THEN TRUE ELSE FALSE END AS has_turning_loop,

        -- Time of day classification
        COALESCE(NULLIF(TRIM(sunrise_sunset), ''), 'Day') AS sunrise_sunset,
        COALESCE(NULLIF(TRIM(civil_twilight), ''), 'Day') AS civil_twilight,

        -- Metadata
        load_timestamp,
        CURRENT_TIMESTAMP() AS dbt_updated_at

    FROM source

    WHERE
        -- Data quality filters
        accident_id IS NOT NULL
        AND start_time IS NOT NULL
        AND TRY_CAST(start_time AS TIMESTAMP_NTZ) IS NOT NULL
        AND state = 'CA'
        AND start_lat BETWEEN 32.5 AND 42.0      -- California latitude range
        AND start_lng BETWEEN -124.5 AND -114.0  -- California longitude range
        AND severity BETWEEN 1 AND 4
)

SELECT *
FROM cleaned
