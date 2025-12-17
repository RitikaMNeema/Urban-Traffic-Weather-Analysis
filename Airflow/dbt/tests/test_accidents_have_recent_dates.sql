
SELECT 
    accident_id,
    accident_date,
    city,
    severity,
    CASE 
        WHEN accident_date > CURRENT_DATE() THEN 'Future date'
        WHEN accident_date < DATEADD(year, -10, CURRENT_DATE()) THEN 'Too old (>10 years)'
        ELSE 'Unknown issue'
    END as failure_reason
FROM {{ ref('stg_traffic_accidents') }}
WHERE 
    accident_date > CURRENT_DATE()
    OR accident_date < DATEADD(year, -10, CURRENT_DATE())