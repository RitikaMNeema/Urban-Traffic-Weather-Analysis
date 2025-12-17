{{
    config(
        materialized='view',
        schema='STAGING',
        tags=['staging', 'traffic', 'realtime']
    )
}}

WITH raw_traffic_flow AS (
    SELECT * FROM {{ source('raw', 'TRAFFIC_FLOW_RAW') }}
),

cleaned AS (
    SELECT
        flow_id,
        city,
        state,
        CAST(timestamp AS TIMESTAMP_NTZ) AS timestamp,
        latitude,
        longitude,
        
        -- Speed metrics
        current_speed AS current_speed_mph,
        free_flow_speed AS free_flow_speed_mph,
        
        -- Calculate speed reduction percentage
        CASE 
            WHEN free_flow_speed > 0 THEN
                ROUND(((free_flow_speed - current_speed) / free_flow_speed) * 100, 2)
            ELSE 0 
        END AS speed_reduction_pct,
        
        -- FIX #2: Add congestion level categorization
        CASE
            WHEN free_flow_speed = 0 THEN 'Unknown'
            WHEN free_flow_speed > 0 AND ((free_flow_speed - current_speed) / free_flow_speed) >= 0.75 THEN 'Severe'
            WHEN free_flow_speed > 0 AND ((free_flow_speed - current_speed) / free_flow_speed) >= 0.50 THEN 'Heavy'
            WHEN free_flow_speed > 0 AND ((free_flow_speed - current_speed) / free_flow_speed) >= 0.25 THEN 'Moderate'
            WHEN free_flow_speed > 0 AND ((free_flow_speed - current_speed) / free_flow_speed) >= 0.10 THEN 'Light'
            ELSE 'Free Flow'
        END AS congestion_level,
        
        -- Travel time metrics
        current_travel_time AS current_travel_time_sec,
        free_flow_travel_time AS free_flow_travel_time_sec,
        
        -- Calculate delay
        current_travel_time - free_flow_travel_time AS delay_sec,
        
        -- Confidence and status
        confidence AS confidence_level,
        COALESCE(road_closure, FALSE) AS road_closure,
        road_name,
        
        -- Metadata
        load_timestamp,
        raw_json
        
    FROM raw_traffic_flow
    WHERE current_speed IS NOT NULL
        AND free_flow_speed IS NOT NULL
        AND current_speed >= 0
        AND free_flow_speed >= 0
)

SELECT * FROM cleaned