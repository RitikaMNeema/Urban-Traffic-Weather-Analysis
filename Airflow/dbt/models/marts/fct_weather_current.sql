{{
    config(
        materialized='table',
        tags=['weather', 'marts', 'realtime']
    )
}}

WITH current_weather AS (
    SELECT * FROM {{ ref('stg_weather_realtime') }}
),

weather_metrics AS (
    SELECT
        -- Identifiers
        record_id,
        city,
        state,
        region,
        county,
        latitude,
        longitude,
        
        -- Time dimensions (using columns from staging)
        timestamp_utc AS weather_timestamp,
        date AS weather_date,
        hour,
        CASE 
            WHEN hour BETWEEN 6 AND 11 THEN 'Morning'
            WHEN hour BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN hour BETWEEN 18 AND 21 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        
        -- Temperature metrics
        temperature_f,
        feels_like_f,
        temp_min_f,
        temp_max_f,
        ROUND(feels_like_f - temperature_f, 1) AS heat_index_difference,
        temperature_category,
        
        -- Additional temperature category for more granularity
        CASE
            WHEN temperature_f < 32 THEN 'Freezing'
            WHEN temperature_f < 50 THEN 'Cold'
            WHEN temperature_f < 65 THEN 'Cool'
            WHEN temperature_f < 75 THEN 'Mild'
            WHEN temperature_f < 85 THEN 'Warm'
            WHEN temperature_f < 95 THEN 'Hot'
            ELSE 'Very Hot'
        END AS temperature_category_detailed,
        
        -- Atmospheric conditions
        pressure_msl_hpa,
        surface_pressure_hpa,
        humidity_pct,
        cloud_cover_pct,
        CASE
            WHEN humidity_pct < 30 THEN 'Dry'
            WHEN humidity_pct < 60 THEN 'Comfortable'
            WHEN humidity_pct < 80 THEN 'Humid'
            ELSE 'Very Humid'
        END AS humidity_category,
        
        -- Wind metrics
        wind_speed_mph,
        wind_direction_deg,
        wind_gusts_mph,
        wind_category,
        CASE
            WHEN wind_direction_deg BETWEEN 0 AND 22.5 THEN 'N'
            WHEN wind_direction_deg BETWEEN 22.5 AND 67.5 THEN 'NE'
            WHEN wind_direction_deg BETWEEN 67.5 AND 112.5 THEN 'E'
            WHEN wind_direction_deg BETWEEN 112.5 AND 157.5 THEN 'SE'
            WHEN wind_direction_deg BETWEEN 157.5 AND 202.5 THEN 'S'
            WHEN wind_direction_deg BETWEEN 202.5 AND 247.5 THEN 'SW'
            WHEN wind_direction_deg BETWEEN 247.5 AND 292.5 THEN 'W'
            WHEN wind_direction_deg BETWEEN 292.5 AND 337.5 THEN 'NW'
            ELSE 'N'
        END AS wind_direction_cardinal,
        
        -- Precipitation
        precipitation_in,
        rain_in,
        snowfall_in,
        precipitation_category,
        CASE
            WHEN precipitation_in > 0 OR rain_in > 0 OR snowfall_in > 0 THEN TRUE
            ELSE FALSE
        END AS has_precipitation,
        
        -- Weather conditions
        weather_code,
        weather_main,
        weather_description,
        weather_icon,
        
        -- Hazard flags from staging
        is_freezing,
        is_high_wind,
        is_heavy_rain,
        has_snow,
        
        -- Comfort index (comprehensive weather quality score)
        CASE
            WHEN temperature_f BETWEEN 65 AND 75 
                AND humidity_pct BETWEEN 30 AND 60 
                AND wind_speed_mph < 15 
                AND precipitation_in = 0
            THEN 'Excellent'
            WHEN temperature_f BETWEEN 55 AND 85 
                AND humidity_pct < 70 
                AND wind_speed_mph < 20
                AND precipitation_in < 0.1
            THEN 'Good'
            WHEN temperature_f BETWEEN 40 AND 95 
                AND wind_speed_mph < 30
                AND precipitation_in < 0.3
            THEN 'Fair'
            ELSE 'Poor'
        END AS comfort_index,
        
        -- Metadata
        data_source,
        load_timestamp
        
    FROM current_weather
),

regional_comparisons AS (
    SELECT
        w.*,
        
        -- Regional temperature comparisons
        AVG(temperature_f) OVER (PARTITION BY region) AS region_avg_temp,
        MAX(temperature_f) OVER (PARTITION BY region) AS region_max_temp,
        MIN(temperature_f) OVER (PARTITION BY region) AS region_min_temp,
        
        -- Regional humidity comparisons
        AVG(humidity_pct) OVER (PARTITION BY region) AS region_avg_humidity,
        MAX(humidity_pct) OVER (PARTITION BY region) AS region_max_humidity,
        MIN(humidity_pct) OVER (PARTITION BY region) AS region_min_humidity,
        
        -- Regional wind comparisons
        AVG(wind_speed_mph) OVER (PARTITION BY region) AS region_avg_wind_speed,
        MAX(wind_speed_mph) OVER (PARTITION BY region) AS region_max_wind_speed,
        
        -- Statewide temperature comparisons
        AVG(temperature_f) OVER () AS state_avg_temp,
        MAX(temperature_f) OVER () AS state_max_temp,
        MIN(temperature_f) OVER () AS state_min_temp,
        
        -- Statewide humidity comparisons
        AVG(humidity_pct) OVER () AS state_avg_humidity,
        
        -- Rankings within region
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY temperature_f DESC) AS region_temp_rank,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY humidity_pct DESC) AS region_humidity_rank,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY wind_speed_mph DESC) AS region_wind_rank,
        
        -- Statewide rankings
        ROW_NUMBER() OVER (ORDER BY temperature_f DESC) AS state_temp_rank,
        ROW_NUMBER() OVER (ORDER BY humidity_pct DESC) AS state_humidity_rank,
        ROW_NUMBER() OVER (ORDER BY wind_speed_mph DESC) AS state_wind_rank
        
    FROM weather_metrics w
)

SELECT
    -- Identifiers
    record_id,
    city,
    state,
    region,
    county,
    latitude,
    longitude,
    
    -- Time
    weather_timestamp,
    weather_date,
    hour,
    time_of_day,
    
    -- Temperature
    temperature_f,
    feels_like_f,
    temp_min_f,
    temp_max_f,
    heat_index_difference,
    temperature_category,
    temperature_category_detailed,
    
    -- Atmospheric
    pressure_msl_hpa,
    surface_pressure_hpa,
    humidity_pct,
    cloud_cover_pct,
    humidity_category,
    
    -- Wind
    wind_speed_mph,
    wind_direction_deg,
    wind_gusts_mph,
    wind_category,
    wind_direction_cardinal,
    
    -- Precipitation
    precipitation_in,
    rain_in,
    snowfall_in,
    precipitation_category,
    has_precipitation,
    
    -- Weather
    weather_code,
    weather_main,
    weather_description,
    weather_icon,
    
    -- Flags
    is_freezing,
    is_high_wind,
    is_heavy_rain,
    has_snow,
    
    -- Comfort
    comfort_index,
    
    -- Regional comparisons
    region_avg_temp,
    region_max_temp,
    region_min_temp,
    region_avg_humidity,
    region_max_humidity,
    region_min_humidity,
    region_avg_wind_speed,
    region_max_wind_speed,
    
    -- State comparisons
    state_avg_temp,
    state_max_temp,
    state_min_temp,
    state_avg_humidity,
    
    -- Rankings
    region_temp_rank,
    region_humidity_rank,
    region_wind_rank,
    state_temp_rank,
    state_humidity_rank,
    state_wind_rank,
    
    -- Calculated differences
    ROUND(temperature_f - region_avg_temp, 1) AS temp_diff_from_region_avg,
    ROUND(temperature_f - state_avg_temp, 1) AS temp_diff_from_state_avg,
    ROUND(humidity_pct - region_avg_humidity, 1) AS humidity_diff_from_region_avg,
    ROUND(wind_speed_mph - region_avg_wind_speed, 1) AS wind_diff_from_region_avg,
    
    -- Metadata
    data_source,
    load_timestamp,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS mart_created_at
    
FROM regional_comparisons