{{
  config(
    materialized='table',
    schema='MARTS',
    tags=['marts', 'dashboard', 'safety', 'historical']
  )
}}

-- ✅ FIX: Check if correlation data exists first
WITH data_check AS (
    SELECT COUNT(*) as record_count 
    FROM {{ ref('accident_weather_correlation') }}
),

accident_correlation AS (
    SELECT * FROM {{ ref('accident_weather_correlation') }}
    WHERE EXISTS (SELECT 1 FROM data_check WHERE record_count > 0)
),

city_severity_summary AS (
    SELECT
        city,
        county,
        state,
        severity,
        severity_label,
        COUNT(*) AS accident_count,
        AVG(temperature_f) AS avg_temperature_f,
        AVG(visibility_mi) AS avg_visibility_mi,
        AVG(wind_speed_mph) AS avg_wind_speed_mph,
        AVG(humidity_pct) AS avg_humidity_pct,
        SUM(CASE WHEN has_weather_hazard THEN 1 ELSE 0 END) AS weather_related_count,
        SUM(CASE WHEN has_infrastructure_risk THEN 1 ELSE 0 END) AS infrastructure_related_count,
        AVG(risk_score) AS avg_risk_score,
        MIN(accident_date) AS earliest_accident,
        MAX(accident_date) AS latest_accident
    FROM accident_correlation
    GROUP BY city, county, state, severity, severity_label
),

weather_condition_summary AS (
    SELECT
        weather_main,
        weather_description,
        COUNT(*) AS accident_count,
        AVG(severity) AS avg_severity,
        AVG(risk_score) AS avg_risk_score,
        SUM(CASE WHEN severity >= 3 THEN 1 ELSE 0 END) AS severe_accident_count
    FROM accident_correlation
    GROUP BY weather_main, weather_description
),

time_pattern_summary AS (
    SELECT
        day_name,
        accident_hour,
        is_weekend,
        sunrise_sunset,
        COUNT(*) AS accident_count,
        AVG(severity) AS avg_severity,
        AVG(risk_score) AS avg_risk_score,
        SUM(CASE WHEN has_weather_hazard THEN 1 ELSE 0 END) AS weather_related_count
    FROM accident_correlation
    GROUP BY day_name, accident_hour, is_weekend, sunrise_sunset
),

high_risk_locations AS (
    SELECT
        city,
        county,
        state,
        street,
        COUNT(*) AS accident_count,
        AVG(severity) AS avg_severity,
        AVG(risk_score) AS avg_risk_score,
        MAX(risk_category) AS max_risk_category,
        SUM(CASE WHEN has_junction THEN 1 ELSE 0 END) AS junction_accidents,
        SUM(CASE WHEN has_traffic_signal THEN 1 ELSE 0 END) AS traffic_signal_accidents,
        SUM(CASE WHEN has_weather_hazard THEN 1 ELSE 0 END) AS weather_related_accidents
    FROM accident_correlation
    GROUP BY city, county, state, street
    HAVING COUNT(*) >= 5
    ORDER BY avg_risk_score DESC
    LIMIT 100
)

-- Final dashboard combining all metrics
SELECT
    'city_severity' AS metric_type,
    cs.city,
    cs.county,
    cs.state,
    cs.severity_label AS category,
    cs.accident_count,
    cs.avg_risk_score,
    cs.weather_related_count,
    cs.infrastructure_related_count,
    NULL AS detail,
    CURRENT_TIMESTAMP() AS dashboard_updated_at
FROM city_severity_summary cs

UNION ALL

SELECT
    'weather_condition' AS metric_type,
    NULL AS city,
    NULL AS county,
    NULL AS state,
    wc.weather_main AS category,
    wc.accident_count,
    wc.avg_risk_score,
    wc.accident_count AS weather_related_count,
    0 AS infrastructure_related_count,
    wc.weather_description AS detail,
    CURRENT_TIMESTAMP() AS dashboard_updated_at
FROM weather_condition_summary wc

UNION ALL

SELECT
    'time_pattern' AS metric_type,
    NULL AS city,
    NULL AS county,
    NULL AS state,
    tp.day_name || ' ' || LPAD(tp.accident_hour::VARCHAR, 2, '0') || ':00' AS category,
    tp.accident_count,
    tp.avg_risk_score,
    tp.weather_related_count,
    0 AS infrastructure_related_count,
    tp.sunrise_sunset AS detail,
    CURRENT_TIMESTAMP() AS dashboard_updated_at
FROM time_pattern_summary tp

UNION ALL

SELECT
    'high_risk_location' AS metric_type,
    hr.city,
    hr.county,
    hr.state,
    hr.street AS category,
    hr.accident_count,
    hr.avg_risk_score,
    hr.weather_related_accidents AS weather_related_count,
    hr.junction_accidents + hr.traffic_signal_accidents AS infrastructure_related_count,
    hr.max_risk_category AS detail,
    CURRENT_TIMESTAMP() AS dashboard_updated_at
FROM high_risk_locations hr

ORDER BY metric_type, avg_risk_score DESC