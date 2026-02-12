--Created a base becasue of the believe that there will be tables to join later on.
WITH source AS (
    SELECT *
    FROM
        {{ source('sligro60_slgfilelib_slgfilelib', 'dwfkrcpf') }}
)

SELECT *
FROM source
WHERE _fivetran_synced > '2024-12-16 10:00:00.000000'
