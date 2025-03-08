WITH source AS (
    SELECT * FROM {{ source('tfl_raw', 'platforms') }}
),

staged AS (
    SELECT
        UniqueId as platform_id,
        StationUniqueId as station_id,
        PlatformNumber as platform_number,
        CardinalDirection as cardinal_direction,
        PlatformNaptanCode as platform_naptan_code,
        FriendlyName as platform_name,
        IsCustomerFacing as is_customer_facing,
        HasServiceInterchange as has_service_interchange,
        AccessibleEntranceName as accessible_entrance_name,
        HasStepFreeRouteInformation as has_step_free_route
    FROM source
)

SELECT * FROM staged 