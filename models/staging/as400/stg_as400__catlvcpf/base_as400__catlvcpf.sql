WITH source AS (
    SELECT *
    FROM {{ source('sligro60_slgfilelib_slgfilelib', 'catlvcpf') }}
)

SELECT *
FROM source
WHERE _fivetran_synced > '2024-06-12 10:00:00.000000'
