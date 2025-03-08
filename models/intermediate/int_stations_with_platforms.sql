WITH stations AS (
    SELECT * FROM {{ ref('stg_tfl_stations') }}
),

platforms AS (
    SELECT * FROM {{ ref('stg_tfl_platforms') }}
),

stations_with_platforms AS (
    SELECT
        s.station_id,
        s.station_name,
        s.fare_zones,
        s.has_wifi,
        s.has_blue_badge_parking,
        s.blue_badge_parking_spaces,
        s.has_taxi_rank,
        s.has_bus_interchange,
        s.has_pier_interchange,
        s.has_rail_interchange,
        s.has_airport_interchange,
        s.has_emirates_airline_interchange,
        ARRAY_AGG(
            STRUCT(
                p.platform_id,
                p.platform_number,
                p.cardinal_direction,
                p.platform_name,
                p.is_customer_facing,
                p.has_service_interchange,
                p.has_step_free_route
            )
        ) as platforms
    FROM stations s
    LEFT JOIN platforms p
        ON s.station_id = p.station_id
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
)

SELECT * FROM stations_with_platforms 