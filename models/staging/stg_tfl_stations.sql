WITH source AS (
    SELECT * FROM {{ source('tfl_raw', 'Stations') }}
),

staged AS (
    SELECT
        UniqueId as station_id,
        Name as station_name,
        FareZones as fare_zones,
        HubNaptanCode as hub_naptan_code,
        Wifi as has_wifi,
        OutsideStationUniqueId as outside_station_id,
        BlueBadgeCarParking as has_blue_badge_parking,
        BlueBadgeCarParkSpaces as blue_badge_parking_spaces,
        TaxiRanksOutsideStation as has_taxi_rank,
        MainBusInterchange as has_bus_interchange,
        PierInterchange as has_pier_interchange,
        NationalRailInterchange as has_rail_interchange,
        AirportInterchange as has_airport_interchange,
        EmiratesAirLineInterchange as has_emirates_airline_interchange
    FROM source
)

SELECT * FROM staged 