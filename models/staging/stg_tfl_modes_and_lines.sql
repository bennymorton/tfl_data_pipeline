WITH source AS (
    SELECT * FROM {{ source('tfl_raw', 'modes_and_lines') }}
),

staged AS (
    SELECT
        Mode as mode_type,
        Name as line_name
    FROM source
)

SELECT * FROM staged 