-- mart_realtime_traffic_dashboard.sql

{{ config(
    materialized = 'table',
    schema = 'MARTS',
    tags = ['marts', 'dashboard', 'realtime', 'traffic']
) }}

WITH traffic_flow AS (
    SELECT * FROM {{ ref('stg_traffic_flow_realtime') }}
),

-- Calculate congestion score since it's not in the staging table
traffic_with_congestion AS (
    SELECT
        *,
        -- Calculate congestion_score based on speed_reduction_pct
        CASE
            WHEN speed_reduction_pct >= 75 THEN 100
            WHEN speed_reduction_pct >= 50 THEN 80
            WHEN speed_reduction_pct >= 25 THEN 50
            WHEN speed_reduction_pct >= 10 THEN 25
            ELSE 10
        END AS congestion_score
        -- ❌ REMOVE the congestion_level calculation here since it's already in staging
    FROM traffic_flow
),

-- Aggregate metrics by city
city_summary AS (
    SELECT
        city,
        state,
        COUNT(*) AS total_segments,
        AVG(current_speed_mph) AS avg_current_speed,
        AVG(free_flow_speed_mph) AS avg_free_flow_speed,
        AVG(speed_reduction_pct) AS avg_speed_reduction_pct,
        AVG(congestion_score) AS avg_congestion_score,
        AVG(delay_sec) AS avg_delay_seconds,
        SUM(CASE WHEN road_closure THEN 1 ELSE 0 END) AS closed_roads_count,
        MAX(timestamp) AS last_updated
    FROM traffic_with_congestion
    GROUP BY city, state
),

-- Latest traffic conditions with enriched data
latest_conditions AS (
    SELECT
        t.flow_id,
        t.city,
        t.state,
        t.road_name,
        t.timestamp,
        t.latitude,
        t.longitude,
        t.current_speed_mph,
        t.free_flow_speed_mph,
        t.speed_reduction_pct,
        t.congestion_score,
        t.congestion_level,  -- ✅ This comes from staging
        t.delay_sec,
        t.confidence_level,
        t.road_closure,

        -- Add city-level context
        cs.avg_congestion_score AS city_avg_congestion,
        cs.total_segments AS city_total_segments,
        cs.closed_roads_count AS city_closed_roads,

        -- Relative congestion
        t.congestion_score - cs.avg_congestion_score AS congestion_vs_city_avg,

        -- Classification
        CASE
            WHEN t.congestion_score >= cs.avg_congestion_score + 20 THEN 'ABOVE_AVG'
            WHEN t.congestion_score <= cs.avg_congestion_score - 20 THEN 'BELOW_AVG'
            ELSE 'AVERAGE'
        END AS relative_congestion_level
    FROM traffic_with_congestion t
    LEFT JOIN city_summary cs
        ON t.city = cs.city
       AND t.state = cs.state
)

-- Final dashboard output
SELECT *
FROM latest_conditions
ORDER BY congestion_score DESC, city, road_name