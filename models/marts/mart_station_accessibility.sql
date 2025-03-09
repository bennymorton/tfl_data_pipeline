WITH stations_platforms AS (
    SELECT * FROM {{ ref('int_stations_with_platforms') }}
),

accessibility_stats AS (
    SELECT
        station_id,
        station_name,
        has_blue_badge_parking,
        blue_badge_parking_spaces,
        has_taxi_rank,
        has_bus_interchange,
        -- Calculate accessibility metrics
        (SELECT COUNT(1) FROM UNNEST(s.platforms) p WHERE p.has_step_free_route) as step_free_platforms,
        ARRAY_LENGTH(s.platforms) as total_platforms,
        SAFE_DIVIDE(
            (SELECT COUNT(1) FROM UNNEST(s.platforms) p WHERE p.has_step_free_route),
            ARRAY_LENGTH(s.platforms)
        ) * 100 as step_free_percentage,
        -- Aggregate platform directions
        ARRAY_AGG(DISTINCT p.cardinal_direction) as platform_directions
    FROM stations_platforms s,
    UNNEST(s.platforms) p
    GROUP BY 1,2,3,4,5,6, s.platforms
)

SELECT
    station_id,
    station_name,
    has_blue_badge_parking,
    blue_badge_parking_spaces,
    has_taxi_rank,
    has_bus_interchange,
    step_free_platforms,
    total_platforms,
    ROUND(step_free_percentage, 1) as step_free_percentage,
    platform_directions,
    -- Calculate overall accessibility score (example methodology)
    (
        CASE WHEN CAST(has_blue_badge_parking AS BOOL) THEN 20 ELSE 0 END +
        CASE WHEN CAST(has_taxi_rank AS BOOL) THEN 20 ELSE 0 END +
        CASE WHEN step_free_percentage = 100 THEN 40
             WHEN step_free_percentage >= 50 THEN 20
             ELSE 0 END
    ) as accessibility_score
FROM accessibility_stats 