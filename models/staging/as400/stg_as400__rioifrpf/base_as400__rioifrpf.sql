WITH source AS (
    SELECT *
    FROM
        {{ source('sligro60_slgfilelib_slgfilelib', 'rioifrpf') }}
)

SELECT *
FROM source
WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
