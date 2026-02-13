COMPLEX_ANALYTICS_QUERY = """
WITH disruption_metrics AS (
    SELECT 
        DATE(start_time) as disruption_date,
        type,
        COUNT(*) as incident_count,
        AVG(CAST(
            (julianday(end_time) - julianday(start_time)) * 1440 
            AS REAL)
        ) as avg_duration_minutes,
        -- Window function: running total per day
        SUM(COUNT(*)) OVER (
            ORDER BY DATE(start_time) 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7day_total
    FROM disruptions
    WHERE start_time >= date('now', '-30 days')
    GROUP BY DATE(start_time), type
),
station_impact AS (
    -- Find most problematic stations
    SELECT 
        station_code,
        COUNT(*) as disruption_count,
        -- Percentile rank
        PERCENT_RANK() OVER (ORDER BY COUNT(*)) as severity_percentile
    FROM (
        SELECT 
            TRIM(value) as station_code
        FROM disruptions,
        -- Split comma-separated stations
        json_each('["' || REPLACE(affected_stations, ',', '","') || '"]')
    )
    GROUP BY station_code
)
SELECT 
    dm.disruption_date,
    dm.type,
    dm.incident_count,
    ROUND(dm.avg_duration_minutes, 2) as avg_duration,
    dm.rolling_7day_total,
    -- Subquery: Most affected station that day
    (
        SELECT si.station_code
        FROM station_impact si
        WHERE si.severity_percentile > 0.9
        LIMIT 1
    ) as worst_station,
    -- CTR calculation: % of disruptions that are cancellations
    ROUND(
        100.0 * SUM(CASE WHEN dm.type = 'cancellation' THEN 1 ELSE 0 END) 
        OVER (PARTITION BY dm.disruption_date) 
        / SUM(dm.incident_count) OVER (PARTITION BY dm.disruption_date),
        2
    ) as cancellation_rate_pct
FROM disruption_metrics dm
ORDER BY dm.disruption_date DESC, dm.incident_count DESC;