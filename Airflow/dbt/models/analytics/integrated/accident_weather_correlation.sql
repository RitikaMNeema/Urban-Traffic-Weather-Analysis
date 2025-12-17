{{
    config(
        materialized='table',
        unique_key='accident_id',
        schema='ANALYTICS',
        tags=['analytics', 'integrated', 'correlation']
    )
}}

WITH accidents AS (
    SELECT * FROM {{ ref('stg_traffic_accidents') }}
),

weather AS (
    SELECT * FROM {{ ref('stg_weather_historical') }}
),

weather_check AS (
    SELECT COUNT(*) as weather_count FROM weather
),

joined AS (
    SELECT
        a.accident_id,
        a.severity,
        a.severity_label,
        a.start_time,
        a.end_time,
        a.accident_date,
        a.accident_hour,
        a.day_of_week,
        a.day_name,
        a.is_weekend,
        
        a.city,
        a.county,
        a.state,
        a.latitude AS accident_lat,
        a.longitude AS accident_lng,
        a.street,
        a.zipcode,
        
        a.distance_affected_mi,
        a.description,
        
        a.has_amenity,
        a.has_bump,
        a.has_junction,
        a.has_traffic_signal,
        a.has_crossing,
        a.has_stop,
        a.has_roundabout,
        a.has_railway,
        a.has_give_way,
        a.has_traffic_calming,
        
        -- Weather fields with COALESCE for safety
        COALESCE(w.temperature_f, 70) AS temperature_f,
        COALESCE(w.temperature_category, 'Moderate') AS temperature_category,
        COALESCE(w.feels_like_f, 70) AS feels_like_f,
        COALESCE(w.humidity_pct, 50) AS humidity_pct,
        COALESCE(w.pressure_in, 29.92) AS pressure_in,
        COALESCE(w.visibility_mi, 10) AS visibility_mi,
        COALESCE(w.visibility_category, 'Good') AS visibility_category,
        COALESCE(w.wind_speed_mph, 0) AS wind_speed_mph,
        COALESCE(w.wind_category, 'Calm') AS wind_category,
        COALESCE(w.wind_deg, 0) AS wind_direction_deg,
        COALESCE(w.weather_main, 'Clear') AS weather_main,
        COALESCE(w.weather_description, 'clear sky') AS weather_description,
        COALESCE(w.total_precip_1h_in, 0) AS total_precip_1h_in,
        COALESCE(w.precipitation_category, 'None') AS precipitation_category,
        COALESCE(w.clouds_pct, 0) AS clouds_pct,
        
        COALESCE(w.is_freezing, FALSE) AS is_freezing,
        COALESCE(w.is_poor_visibility, FALSE) AS is_poor_visibility,
        COALESCE(w.is_high_wind, FALSE) AS is_high_wind,
        COALESCE(w.is_heavy_rain, FALSE) AS is_heavy_rain,
        COALESCE(w.has_snow, FALSE) AS has_snow,
        
        a.sunrise_sunset,
        a.civil_twilight,
        
        -- Risk score calculation
        (
            CASE WHEN a.severity >= 3 THEN 3 ELSE a.severity END +
            CASE WHEN COALESCE(w.is_poor_visibility, FALSE) THEN 2 ELSE 0 END +
            CASE WHEN COALESCE(w.is_heavy_rain, FALSE) THEN 2 ELSE 0 END +
            CASE WHEN COALESCE(w.has_snow, FALSE) THEN 2 ELSE 0 END +
            CASE WHEN COALESCE(w.is_freezing, FALSE) THEN 1 ELSE 0 END +
            CASE WHEN COALESCE(w.is_high_wind, FALSE) THEN 1 ELSE 0 END +
            CASE WHEN a.sunrise_sunset = 'Night' THEN 1 ELSE 0 END
        ) AS risk_score,
        
        CASE 
            WHEN (
                CASE WHEN a.severity >= 3 THEN 3 ELSE a.severity END +
                CASE WHEN COALESCE(w.is_poor_visibility, FALSE) THEN 2 ELSE 0 END +
                CASE WHEN COALESCE(w.is_heavy_rain, FALSE) THEN 2 ELSE 0 END +
                CASE WHEN COALESCE(w.has_snow, FALSE) THEN 2 ELSE 0 END +
                CASE WHEN COALESCE(w.is_freezing, FALSE) THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(w.is_high_wind, FALSE) THEN 1 ELSE 0 END +
                CASE WHEN a.sunrise_sunset = 'Night' THEN 1 ELSE 0 END
            ) <= 3 THEN 'Low Risk'
            WHEN (
                CASE WHEN a.severity >= 3 THEN 3 ELSE a.severity END +
                CASE WHEN COALESCE(w.is_poor_visibility, FALSE) THEN 2 ELSE 0 END +
                CASE WHEN COALESCE(w.is_heavy_rain, FALSE) THEN 2 ELSE 0 END +
                CASE WHEN COALESCE(w.has_snow, FALSE) THEN 2 ELSE 0 END +
                CASE WHEN COALESCE(w.is_freezing, FALSE) THEN 1 ELSE 0 END +
                CASE WHEN COALESCE(w.is_high_wind, FALSE) THEN 1 ELSE 0 END +
                CASE WHEN a.sunrise_sunset = 'Night' THEN 1 ELSE 0 END
            ) <= 6 THEN 'Medium Risk'
            ELSE 'High Risk'
        END AS risk_category,
        
        CASE 
            WHEN COALESCE(w.is_poor_visibility, FALSE) = TRUE 
                OR COALESCE(w.is_heavy_rain, FALSE) = TRUE 
                OR COALESCE(w.has_snow, FALSE) = TRUE 
                OR COALESCE(w.is_freezing, FALSE) = TRUE 
            THEN TRUE 
            ELSE FALSE 
        END AS has_weather_hazard,
        
        CASE 
            WHEN a.has_junction = TRUE 
                OR a.has_roundabout = TRUE 
                OR a.has_railway = TRUE 
            THEN TRUE 
            ELSE FALSE 
        END AS has_infrastructure_risk,
        
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ run_started_at }}' AS dbt_run_timestamp
        
    FROM accidents a
    LEFT JOIN weather w
        ON a.city = w.city
        AND a.accident_date = w.date
        AND a.accident_hour = w.hour
    CROSS JOIN weather_check wc
    WHERE wc.weather_count > 0
)

SELECT * FROM joined