WITH source AS (
    SELECT * FROM {{ source('tfl_raw', 'ModesAndLines') }}
),

staged AS (
    SELECT
        string_field_0 as mode_type,
        string_field_1 as line_name
    FROM source
)

SELECT * FROM staged 